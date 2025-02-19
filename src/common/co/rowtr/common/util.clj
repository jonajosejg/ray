(ns co.rowtr.common.util
  (:require
    [cemerick.bandalore                     :as sqs]
    [taoensso.carmine                       :as car]
    [co.rowtr.geo-cache.redis-cache         :as red]
    [clj-time.format                        :as f]
    [clojure.string                                   :refer [trim]]
    [clj-time.core                          :as t]
    [clj-time.coerce                        :as c]
    [clojure.tools.logging                  :as log]))

(def custom-fmt (f/formatter "MM/dd/YYYY HH:mm:ss"))
(def time-only  (f/formatter "HH:mm:ss"))
(def time-and-meridian (f/formatter "HH:mm:ss a"))
(def utc-fmt    (f/formatters :date-hour-minute-second))
(def multiparse (fn [& [tz]] (f/formatter
                  (or tz (t/default-time-zone))
                  (f/formatter "MM/dd/YY HH:mm:ss")
                  time-and-meridian
                  custom-fmt
                  time-only
                  (f/formatters :date-time)
                  (f/formatters :date-time-no-ms)
                  (f/formatters :date-hour-minute-second))))


(def fmt-local  (fn [fmt dt tz] (when dt (f/unparse-local fmt (c/to-local-date-time (t/to-time-zone (c/to-date-time dt) tz))))))

(def keys-to-fmt-local #{:eta :real-eta :departure-time :eta-in-traffic :real-eta-in-traffic :departure-time-in-traffic})


(def safe-add #((fnil + 0 0) %1 %2))

(defn make-date-from-parts [dt tm tz]
  (try
    (when-not (and (nil? dt) (nil? tm))
      (c/to-sql-date (f/parse (multiparse (t/time-zone-for-id tz)) (trim (str dt " " tm)))))
    (catch Exception _)))

(defn safe-nth
  ([coll index]  (safe-nth coll index nil))
  ([coll index not-found]
    (try  (nth coll index not-found)  (catch Exception _ not-found))))

(defn seconds->hhmmss [n]
  (let [quot*         #(partial quot %)
        mod*          #(partial mod %)
        n             (or n 0)
        [hrs mins]    ((juxt (quot* n) (mod* n)) 3600)
        [mins secs]   ((juxt (quot* mins) (mod* mins)) 60)]
    (format "%02d:%02d:%02d" hrs mins secs)))

(defn meters->miles [n]
  (let [n             (or n 0.0)]
    (format "%.2f mi." (double (/ n 1609.34)))))

(defn prepare-scenario-for-rest [data]
  (let [tz          (try (t/time-zone-for-id (-> data :config :tz_id)) (catch Exception _ (t/default-time-zone)))
        stops       (-> data :solution :stops)
        stops       (mapv
                      #(reduce
                        (fn [m me]
                          (assoc m (key me)
                            (if-not
                              (keys-to-fmt-local (key me))
                              (val me)
                              (try (fmt-local utc-fmt (val me) tz) (catch Exception e (log/error (.getMessage e)))))))
                            {}
                            %)
                      stops)]
    (assoc-in data [:solution :stops] stops)))

(comment
  (mapv #(assoc % :eta (when
                                           (:eta %)
                                           (try
                                            (f/unparse-local utc-fmt (c/to-local-date-time (t/to-time-zone (c/to-date-time (:eta %)) tz)))
                                            (catch Exception e
                                              (log/error (.getMessage e))))))
                          stops)
  )
(defn prepare-solution-for-rest [data]
  (let [tz          (try (t/time-zone-for-id (-> data :decorated :config :tz_id)) (catch Exception _ (t/default-time-zone)))
        routes      (-> data :solution :routes)
        routes      (mapv
                      (fn [r]
                        (assoc
                          r
                          :stops  (mapv
                                    #(reduce
                                      (fn [m me]
                                        (assoc  m (key me)
                                          (if-not
                                            (keys-to-fmt-local (key me))
                                            (val me)
                                            (fmt-local utc-fmt (val me) tz))))
                                      {}
                                      %)
                                    (:stops r))))
                      routes)]
    (assoc-in data [:solution :routes] routes)))

(defn check-cache-for-older-than [{:keys [boundary cnt curs]}]
  (let [cnt         (or cnt 10000)
        curs        (atom (or curs "0"))
        cmp         (c/from-string boundary)
        matches     (atom [])]
    (loop [cursor @curs]
      (let [sc      (red/wcar* (car/scan cursor "COUNT" cnt))
            nxt     (first sc)
            vs      (second sc)
            v       (red/wcar* (apply car/mget vs))
            vv      (mapv #(assoc %1 :key %2) v vs)
            old     (filterv #(or (nil? (:cache-date %)) (t/before? (c/from-date (:cache-date %)) cmp)) vv)]
        (reset! curs nxt)
        (swap! matches concat old)
        (when-not (or (= nxt "0") (< 100000 (count @matches))) (recur nxt))))
    {:nxt @curs  :matches (into [] @matches)}))

(defn check-cache-for-bad-values []
  (let [cnt         100000
        mtch        "edge:*"
        bad         (atom [])]
    (loop [cursor "0"]
      (let [sc      (red/wcar* (car/scan cursor "MATCH" mtch "COUNT" cnt))
            nxt     (first sc)
            vs      (second sc)
            v       (red/wcar* (apply car/mget vs))
            vv      (mapv #(assoc %1 :key %2) v vs)
            junkn   (filterv #(or (nil? (:distance %)) (nil? (:duration %))) vv)
            junkm   (filterv #(or (= (:distance %) Integer/MAX_VALUE) (= (:duration %) Integer/MAX_VALUE)) vv)
            ]
        (swap! bad concat junkn junkm)
        (when (seq junkn) (red/wcar* (apply car/del (map :key junkn))))
        (when (seq junkm) (red/wcar* (apply car/del (map :key junkm))))
        (when-not (= nxt "0") (recur nxt))))
    @bad))

(defmacro with-elapsed-time [expr]
  `(let  [start#      (. System  (nanoTime))
          ret#        ~expr
          elapsed#    (/  (double  (- (. System (nanoTime)) start#)) 1000000000.0)]
     {:elapsed-time elapsed# :return ret#}) )

(def genid #(str (java.util.UUID/randomUUID)))
