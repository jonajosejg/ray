(ns co.rowtr.tsp-api.infile
  (:require
    [co.rowtr.common.config               :as cfg       :refer [s3-creds]]
    [co.rowtr.common.util                 :as u         :refer [genid]]
    [co.rowtr.common.data                 :as d]
    [clojure.data.csv                     :as csv]
    [clojure.tools.logging                :as log]
    [clojure.set                          :as s         :refer [difference rename-keys intersection map-invert]]
    [clojure.java.io                      :as io        :refer [reader]]
    [amazonica.aws.s3                     :as s3]))

(defn make-full-address [m]
  (str (:address1 m)
    (when (not= "" (:address2 m)) (str ", " (:address2 m)))
    (when (not=  "" (:city m)) (str ", " (:city m)))
    (when (not= "" (:state m)) (str ", " (:state m)))
    (when (not= "" (:postalcode m)) (str ", " (:postalcode m)))))

(defn rename-columns [cv m]
 (mapv
   (fn [x]
     (let [nk (:col_key (first (filter (fn [y] (contains? (into #{} (:col_names y)) x)) m)))] (or nk x))) cv) )

(defn fetch-from-s3 [{:keys [bucket key]}]
  (let [obj               (s3/get-object s3-creds bucket key)]
    (csv/read-csv (reader (:input-stream obj)))))

(defn get-keyword-columns [data]
  (mapv keyword (first data)))

(defn cmp-columns [cols mapping]
  (let [c         (into #{} cols)
        m         (into #{} mapping)]
    (seq (difference c m))))

(defn get-numeric-value [string]
  (let [intval        (try (Integer/valueOf string) (catch Exception _))
        floatval      (when-not intval (try (Float/valueOf string) (catch Exception _)))]
    (or intval floatval)))

(defn map-columns [data problem]
  (let [mappings      (d/get-mappings (:idcustomer problem))
        fcols         (get-keyword-columns data)
        mapping       (first (filter #(let [mp (-> % :column_map keys)] (nil? (cmp-columns fcols mp))) mappings))
        op-fields     (when mapping (reduce (fn [r [k v]] (assoc r k (keyword v))) {} (remove #(= (val %) "none") (:column_map mapping))))
        data          (when mapping (mapv #(zipmap (get-keyword-columns data) %) (rest data)))
        data          (when mapping
                        (mapv
                          (fn [d]
                            (let [row                 d
                                  ks                  (rename-keys (select-keys row (keys op-fields)) op-fields)
                                  load                (if-not (:load ks) 1 (get-numeric-value (:load ks)))
                                  load_1              (if-not (:load_1 ks) 0 (get-numeric-value (:load_1 ks)))
                                  svc                 (if-not (:duration ks) 0 (get-numeric-value (:duration ks)))
                                  time-start          (u/make-date-from-parts
                                                        (:window_date ks)
                                                        (:time_start ks)
                                                        (-> problem :depot :tz_id))
                                  time-end            (u/make-date-from-parts
                                                        (:window_date ks)
                                                        (:time_end ks)
                                                        (-> problem :depot :tz_id))
                                  address             (or (:full-address ks) (make-full-address ks))]
                              (assoc ks
                                     :meta row
                                     :id (genid)
                                     :address address
                                     :load load
                                     :load_1 load_1
                                     :time_start time-start
                                     :time_end time-end
                                     :duration svc)))
                          data))]
    (when data {:data data :mapping (:idmapping mapping)})))
