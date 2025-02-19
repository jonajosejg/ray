(ns co.rowtr.tsp-api.middleware
 (:require
    [geo.spatial                            :as spatial :refer [spatial4j-point distance circle intersects?]]
    [clj-time.core                          :as t]
    [clj-time.coerce                        :as c]
    [clj-time.format                        :as f]
    [medley.core                            :as med     :refer [distinct-by]]
    [clojure.pprint                                     :refer [pprint]]
    [clojure.tools.logging                  :as log]
    [clojure.set                                        :refer [superset? subset?]]
    [co.rowtr.geo-graph.graph               :as g       :refer [vertices->edges intra]]
    [co.rowtr.geo-graph.google              :as graph]
    [co.rowtr.tsp.capacity-cluster          :as cc]
    [co.rowtr.tsp.ant-colony                :as ac]
    [co.rowtr.tsp.solution                  :as sol]
    [co.rowtr.tsp.scheme.time-window        :as twin]
    [co.rowtr.common.core                   :as core    :refer [gfn wfn graph-logger]]
    [co.rowtr.common.data                   :as d]
    [co.rowtr.common.util                   :as u       :refer [safe-nth safe-add multiparse]]
    [co.rowtr.common.config                 :as cfg     :refer [ant-colony-trials google-options]]))

(defn round2
  "Round a double to the given precision (number of significant digits)"
  [precision d]
  (let  [factor  (Math/pow 10 precision)]
    (/  (Math/round  (* d factor)) factor)))

(defn handler [request]
  (let [config    (dissoc (:config request) :from-tz-fn)]
    (assoc request :time_end (System/currentTimeMillis) :config config)))

;;;;;;;;;;;;;;;;;;;;; wrap-start
(defn wrap-start [handler]
  (fn [request]
    (let [req     (assoc request :time_start (System/currentTimeMillis))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;; wrap-geocode
(defn wrap-geocode [handler]
  (fn [request]
    (let [req     (assoc request :orders
                             (mapv
                               #(if-not
                                  (and (:lat %) (:lng %))
                                  (let [coords  (try
                                                  (gfn (merge (select-keys % [:address]) google-options {:logger graph-logger}))
                                                  (catch Throwable e {}))]
                                    (merge % (select-keys coords [:lat :lng])))
                                  %)
                               (:orders request)))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;;;; wrap-centroid
(defn compute-centroid [coll]
  (let [latc    (/ (double (reduce + (map :lat coll))) (count coll))
        lngc    (/ (double (reduce + (map :lng coll))) (count coll))]
    {:lat (if (Double/isNaN latc) 0.0 latc) :lng (if (Double/isNaN lngc) 0.0 lngc)}))

(defn wrap-centroid [handler]
  (fn [request]
    (let [coll    (filter #(and (:lat %) (:lng %)) (:orders request))
          req     (assoc request :centroid (compute-centroid coll))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;;;; wrap-bearing
(defn wrap-bearing [handler]
  (fn [request]
    (let [centroid  (:centroid request)
          req       (assoc request :orders
                      (mapv #(assoc % :bearing (when (and centroid (:lat %) (:lng %)) (cc/bearing centroid %))) (:orders request)))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;   wrap-spatial4j-point
(defn make-point
  ([{:keys [lat lng]}]
   (make-point lat lng))
  ([lat lng]
    (when (and lat lng) (spatial4j-point lat lng))))

(defn wrap-spatial4j-point [handler]
  (fn [request]
    (let [req       (assoc request :orders (mapv #(assoc % :spatial4j-point (make-point %)) (:orders request)))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;;; wrap-distance
(defn wrap-distance [handler]
  (fn [request]
    (let [c         (:centroid request)
          cp        (when c (spatial4j-point (:lat c) (:lng c)))
          pfn       (when cp (partial distance cp))
          dfn2      #(let [{:keys [lat lng]} %
                                    pt                (when (and lat lng) (spatial4j-point lat lng))] (when pt (pfn pt)))
          dfn       (when cp #(if (:spatial4j-point %) (pfn (:spatial4j-point %)) (dfn2 %)))
          req       (when cp (assoc request :orders
                                    (mapv #(assoc % :distance (round2 2 (dfn %))) (:orders request))))]
      (handler (or req request)))))

;;;;;;;;;;;;;;;;;;;;;; wrap-adjacent
(defn get-overlapping [coll]
  (let [intervals     (into [] (sort-by :start (map :time-window coll)))
;       d             (;rintln intervals)
        overlap?      (fn [a b]
                        (if (or (nil? a) (nil? b))
                          true
                          (let [i0 (max (-> a :start) (-> b :start))
                                i1 (min (-> a :end) (-> b :end))]
                            (<= i0 i1))))
        combine       (fn [a b] {:start (min (:start a) (:start b)) :end (max (:end a) (:end b))})
        contains?     (fn [a b]  (and (<= (:start a) (:start b)) (>= (:end a) (:end b))))
        intervals     (filterv
                        identity
                        (reduce
                          (fn [a b]
                            (if
                              (nil? (last a))
                              (conj a b)
                              (if
                                (overlap? (last a) b)
                                (let [c (combine (last a) b)]
                                  (conj (into [] (butlast a)) c))
                                (conj a b))))
                          []
                          intervals))
        groups        (mapv (fn [i] (filterv #(contains? i (:time-window %)) coll)) intervals)]
    (if
      (every? nil? (map :time-window coll))
      (into [] coll)
      (into [] (flatten (filterv #(< 1 (count %)) groups))))))

(defn wrap-adjacent [handler]
  (fn [request]
    (let [prox      (or (-> request :config :proximity) 25)
          orders    (:orders request)
          as        (mapv #(let [cir (circle (make-point %) prox)]
                             (into #{} (filterv identity (mapv (fn [o] (when (intersects? (make-point o) cir) o)) orders)))) orders)
          as        (into [] (distinct  (filterv #(< 1 (count %)) as)))
;         d         (println as)
          as        (mapv get-overlapping as)
          as        (filterv (comp identity seq) as)
          req       (when (seq as) (assoc request :adjacent as))]
;     (when (seq as) (println as))
      (handler (or req request)))))

;;;;;;;;;;;;;;;;;;;;; wrap-config
(defn wrap-config [handler]
  (fn [request]
    (let [config    (or (:config request) (select-keys request [:route_start :trials :lines :route_type :risk_limit :weight]))
          config    (merge config {:route_type (or (:route_type config) :round-trip) :weight (or (:weight config) :distance)})
          tz        (or (:tz_id (:depot request)) (d/get-tzid (:depot request)))
          config    (merge config {:tz_id tz})
          req       (assoc request :config config)]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;; wrap-time-window
(defn diff-in-seconds [tz start end]
  (let [s      (try (f/parse (multiparse tz) start) (catch Exception _))
        e      (try (f/parse (multiparse tz) end) (catch Exception _))
        diff  #(if
               (t/before? s e)
               (t/in-seconds (t/interval s e))
               (* -1 (t/in-seconds (t/interval e s))))]
    (when-not (or (nil? s) (nil? e)) (diff))))

(defn make-time-window [differ order]
  (let [s         (:time_start order)
        e         (:time_end order)
        tw        (when (or s e )
                    {:time-window {:start (differ s) :end (differ e)}})]
    (merge order (when (:time-window tw) tw))))

(defn wrap-time-window [handler]
  (fn [request]
    (let [tz      (t/time-zone-for-id (-> request :config :tz_id))
          shift   (try (f/parse (multiparse tz) (-> request :config :route_start)) (catch Exception _))
          differ  (when shift (partial diff-in-seconds tz (-> request :config :route_start)))
          tw?     (reduce (fn [b m] (or b (:time_start m) (:time_end m))) false (:orders request))
          trucks  (count (-> request :trucks))
          req     (when (and tw? differ)
                    (assoc request
                          :config (assoc (:config request) :time-window true)
                          :orders (mapv #(make-time-window differ %) (:orders request))))
          req     (when req (assoc req :tw-histogram (into {} (map #(assoc {} (first %) (/ (val %) (* 1.0 trucks))) (frequencies (map :time-window (:orders req)))))))]

      (handler (or req request)))))

;;;;;;;;;;;;;;;;;;;;;; wrap-load
(defn wrap-orders [handler & [deflt]]
  (fn [request]
    (let [deflt   (or deflt 1)
          req     (assoc request :orders
                      (mapv #(assoc %
                                    :id (or (:id %) (str (java.util.UUID/randomUUID)))
                                    :service-time (or (:duration %) (:service-time %) 0)
                                    :load (or (:load %) deflt))
                            (:orders request)))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;;;  wrap-capacity
(defn wrap-trucks [handler]
  (fn [request]
    (let [trucks  (or (:trucks request) [{}])
          b       (quot (reduce + (map :load (:orders request))) (count trucks))
          bint    (int (Math/ceil b))
          bmax    (int (Math/ceil (* 1.40 b)))
         ;bmin    (int (Math/floor (* 0.60 b)))
          bmin    0
          deflt   (count (:orders request))
          req     (assoc request :trucks
                      (mapv #(assoc % :id (or (:id % (str (java.util.UUID/randomUUID))))
                                      :bmax bmax
                                      :factor (or (:factor %) 1.0)
                                      :bmin bmin
                                      :balanced bint
                                      :capacity (or (:capacity %) b))
                            (:trucks request)))]
      (handler req))))

;;;;;;;;;;;;;;;;;;;;;;  wrap-cluster
(defn find-closest-cluster [item clusters]
  (let [dfn       (partial distance (make-point item))
        loadadd   #(vector (safe-add (first %1) (first %2)) (safe-add (second %1) (second %2)))
        levelup   #(try
                     (swap! (:level %1) loadadd [(:load %2) (:load_1 %2)])
                     %1
                     (catch java.lang.IllegalStateException e nil))
        clusters  (sort-by #(dfn (make-point %)) clusters)]
    (some identity (map #(levelup % item) clusters))))

(defn compute-cluster-weight [cluster orders]
  (let [os (filterv #(contains? (:orders cluster) (:id %)) orders)
        d  (partial distance (make-point cluster))
        w  (reduce + (map #(d (make-point %)) os))]
    (assoc cluster :weight (round2 0 (reduce + (mapv #(d (make-point %)) os))))))

(defn compute-cluster-centroid [cluster orders]
  (let [c (compute-centroid (filterv (fn [o] (contains? (:orders cluster) (:id o))) orders))]
    (merge cluster c)))

(defn compute-cluster-load [cluster orders]
  (assoc cluster :load (apply + (map :load orders))))

(defn compute-cluster-histogram [cluster orders]
  (let [os  (filterv #(contains? (:orders cluster) (:id %)) orders)
        hg  (frequencies (map :time-window os))]
    (assoc cluster :histogram hg)))

(defn compute-mse [clusters]
  (let [errors    (apply concat (map :errors clusters))
        square    #(* % %)
        mse       (when (seq errors) (/ (reduce + (map square errors)) (count errors)))]
    (or mse 0.0)))

(defn compute-balance-mse [clusters]
  (let [cnt       (count clusters)
        stops     (reduce + (map #(count (:orders %)) clusters))
        bal       (/ (* 1.0 stops) cnt)
        square    #(* % %)
        errors    (mapv #(square (- (-> % :orders count) bal)) clusters)
        mse       (/ (reduce + errors) (count errors))]
    (if mse (round2 2 mse) 0.0)))

(defn compute-cluster-histogram-errors [cluster histogram]
  (let [res       (mapv
                    (fn [h]
                      (let [subtrahend      (or (first (filter #(= (first h) (first %)) (:histogram cluster))) [(first h) 0.0])
                            diff            (- (second h) (second subtrahend))]
                        diff))
                    histogram)]
    (assoc cluster :errors res)))

(defn new-cluster [lat lng cap m]
  (assoc  {}
          :id               (str (java.util.UUID/randomUUID))
          :lat              lat
          :lng              lng
          :bmin             m
          :orders           #{}
          :level            (atom [0 0] :validator (fn [lvl]
                                                     (let [lvl  (if (vector? lvl) lvl [lvl])]
                                                     (and
                                                        (<= (first lvl) (first cap))
                                                        (<= (or (second lvl) 0) (or (second cap) Long/MAX_VALUE))))))))

(defn assign-to-clusters [clusters orders histogram]
  (let [orders    (into [] (reverse (sort-by :load (shuffle orders))))
        clusters    (mapv #(do (reset! (:level %) [0 0])  (assoc % :orders (atom #{}))) clusters)]
    (mapv #(let [c (find-closest-cluster % clusters)] (when c (swap! (:orders c) conj (:id %)))) orders)
    (mapv #(let [c (assoc % :orders @(:orders %))]
             (-> c
                 (compute-cluster-centroid orders)
                 (compute-cluster-weight orders)
                 (compute-cluster-histogram orders)
                 (compute-cluster-load orders)
                 (compute-cluster-histogram-errors histogram)))
          clusters)))

(defn kmeans-cluster [request]
  (let [trucks    (:trucks request)
        hist      (:tw-histogram request)
        limit     100
        orders    (into [] (reverse (sort-by :load (shuffle (:orders request)))))
        means     (shuffle (distinct (mapv #(select-keys % [:lat :lng]) orders)))
        k         (count trucks)
        cap0      (try (apply max (map :capacity trucks)) (catch Exception _ 0))
        cap1      (try (apply max (map :capacity_1 trucks)) (catch Exception _ nil))
        clusters  (mapv #(new-cluster
                           (:lat %1)
                           (:lng %1)
                           (filterv identity [cap0 cap1])
                           (:bmin %2))
                        (take k means) trucks)
        clusters  (assign-to-clusters clusters orders hist)]
;   (clojure.pprint/pprint (:adjacent request))
    (loop [clusters (vector clusters) i 0]
      (let [cfn          (fn [cs] (into #{} (map :orders cs)))
            nclusters    (assign-to-clusters (last clusters) orders hist)]
        (cond
          ;;;;;;;;;;;;;  terminal condition
          (or (= i limit) (= (cfn (last clusters)) (cfn nclusters)))
            (let [weight    (round2 0 (reduce + (mapv :weight (last clusters))))
                  adj?      (fn [a] (let [s (into #{} (map :id a))] (some #(subset? s (:orders %)) (last clusters))))
                  bmin?     #(<= (:bmin %) (count (:orders %)))
                  c         (mapv #(assoc %1 :capacity (:capacity %2)) (sort-by :load (last clusters)) (sort-by :capacity trucks))
                  conforms? (and (every?  adj? (:adjacent request))
                                 (every? #(< (:load %) (:capacity %)) c)
                                 (every?  bmin? (last clusters)))
                  clusters  (mapv #(into #{} (mapv (fn [c] (select-keys c [:id :orders :weight :histogram :errors])) %)) clusters)
                  resp      (assoc {} :cluster-history  clusters
                                        :cluster-weight weight
                                        :balance-mse (compute-balance-mse (last clusters))
                                        :mse (round2 2 (compute-mse (last clusters))))]
              (if conforms? (assoc resp :conforms? true) resp ))

          :else                                     (recur  (conj clusters nclusters) (inc i)))))))

(defn wrap-cluster [handler]
  (fn [request]
    (if (< 1 (count (:trucks request)))
      (let [trials      (or (-> request :config :cluster-trials) 50)
            clusters    (into [] (distinct-by #(-> % :cluster-history last)
                                              (sort-by (juxt :mse :balance-mse :cluster-weight)
                                                       (take trials (repeatedly #(kmeans-cluster request))))))
            clusters    (concat (filter :conforms? clusters) (remove :conforms? clusters))
            req         (assoc request :clusters (into [] clusters))]
        (when-not (seq clusters) (throw (ex-info "Clustering Failure" {:reason "No Conforming Clusters"})))
        (handler req))
      (handler request))))

;;;;;;;;;;;;;;;;;;;;;;  wrap-assign
(defn assign-orders [cluster orders trucks]
  (let [unassigned      (atom cluster)
        conforms?       (fn [c t]  (when (<= (count (:orders c)) (:capacity t)) c))
        trucks          (mapv (fn [t] (let [c (some #(conforms? % t) (reverse (sort-by #(-> % :orders count) @unassigned)))]
                                (swap! unassigned disj c)
                                (assoc t :orders (:orders c))))
                                  (sort-by :factor trucks))]
    (mapv (fn [o] (let [t (some (fn [t] (when (contains? (:orders t) (:id o)) t)) trucks)]
                    (assoc o :truck (:id t) :service-time (int (* (:factor t) (:service-time o)))))) orders)))

(defn unwrap-assign [request & [level]]
  (if
      (< 1 (count (:trucks request)))
      (let [level       (or level 0)
            cluster     (-> (nth (:clusters request) level) :cluster-history last)
;           cluster     (-> request :clusters first :cluster-history last)
            req         (assoc request :orders (assign-orders cluster (:orders request) (:trucks request)))]
        req)
      (let [tid         (-> request :trucks first :id)
            orders      (mapv #(assoc % :truck tid) (:orders request))
            req         (assoc request :orders orders)]
        req)))

(defn wrap-assign [handler]
  (fn [request]
    (if
      (< 1 (count (:trucks request)))
      (let [cluster     (-> request :clusters first :cluster-history last)
            req         (assoc request :orders (assign-orders cluster (:orders request) (:trucks request)))]
        (handler req))
      (let [tid         (-> request :trucks first :id)
            orders      (mapv #(assoc % :truck tid) (:orders request))
            req         (assoc request :orders orders)]
        (handler req)))))

;;;;;;;;;;;;;;;;;;;;;;  wrap-solve
(defn get-augmented-weights [stops & [tuples]]
  (let [tuples      (or
                      tuples
                      (filter
                        #(let [[x y] %] (not= x y))
                        (for [x (range (count stops)) y (range (count stops))] [x y])))
        durations   (mapv :service-time stops)
        options     (merge google-options {:logger graph-logger})]
    (apply
      merge
      (map
        #(let [[f t] %
              d       (or (safe-nth durations f 0) 0)
              from    (nth stops f)
              to      (nth stops t)
              w       (wfn {:from from :to to :options options})]
            (assoc {} (vec %) (assoc w :duration (+ d (:duration w)))))
        tuples))))

(defn google-graph [stops]
    (g/make-concurrent-graph (count stops) (get-augmented-weights stops)))

(defn map-stops [stops orders]
  (mapv #(nth orders (dec %)) stops))

(def make-tz-fmt #(f/formatter :date-time-no-ms %))

(defn compute-eta [start {:keys [cum-dur cum-wait cum-svc] :or {cum-dur 0 cum-wait 0 cum-svc 0}}]
  (when start
    (c/to-sql-date (t/plus  start (t/seconds cum-dur) (t/seconds cum-wait) (t/seconds cum-svc)))))

(defn route-in-future? [start]
  (t/after? start (t/now)))

(defn decorate-traffic [start stops tz]
; (clojure.pprint/pprint stops)
  (if-not
    (route-in-future? start)
    stops
    (let [stops     (reduce
                      (fn [v s2]
                        (let [s1          (last v)
                              to-int      #(when % (quot (c/to-long %) 1000))
                              dep         (or (:departure-time-in-traffic s1) start)
                              dep         (when dep (to-int dep))
                              svc         (or (:service-time s1) 0)
                              w           (when s1
                                            (core/weight {:from    (select-keys s1 [:lat :lng])
                                                          :to      (select-keys s2 [:lat :lng])
                                                          :options (merge google-options {:departure_time (+ dep) :logger graph-logger})}))
                              real-eta    (when w (safe-add dep (:duration_in_traffic w)) )
                              start       (when (:time_start s2) (to-int (f/parse (multiparse tz) (:time_start s2))))
                              wait        (when real-eta (try (- start real-eta) (catch Exception _ 0)))
                              eta         (when wait (if (< 0 wait) start real-eta))
                              dep         (when eta (c/to-sql-date (c/to-date-time (* (safe-add eta (:service-time s2)) 1000))))
                              s2          (assoc s2   :driving-time-in-traffic      (:duration_in_traffic w)
                                                      :departure-time-in-traffic    dep
                                                      :waiting-time-in-traffic      (if (and wait (< 0 wait)) wait 0)
                                                      :real-eta-in-traffic          (when real-eta (c/to-sql-date (c/to-date-time (* 1000 real-eta))))
                                                      :eta-in-traffic               (when eta (c/to-sql-date (c/to-date-time (* eta 1000)))))]
                          (conj v s2)))
                      []
                      stops)]
      stops)))

(defn decorate-stops [r p traffic]
  (let [traffic             false ;; 10/11/18 turning off until we figure out how to use it cost effectively
        tz                  (t/time-zone-for-id (-> p :config :tz_id))
        stops               (map-stops (:stops r) (:orders p))
        stops               (if (= :round-trip (-> p :config :route_type)) (conj stops (:depot p)) stops)
        to-int              #(when % (quot (c/to-long %) 1000))
        start               (try (f/parse (multiparse tz) (-> p :config :route_start)) (catch Exception _))
        trip                (filter identity (concat (vector (:depot r)) stops (vector (:terminal r))))
        waits               (or (:waits r) (repeatedly #(identity 0)))
        durs                (mapv #(int (* (or  (-> p :trucks first :factor) 1.0) (or (:service-time %) 0))) trip)
        idur                (mapv #(- %1 %2) (:idur r)  (cons 0 durs))
;       d                   (println (:idur r))
;       d                   (println durs)
;       d                   (println idur)
        stops               (mapv #(assoc %1 :driving-distance %2 :driving-time %3 :service-time %4) stops (:idis r) idur durs)
        stops               (reduce
                              (fn [v s2]
                                (let [s1        (last v)
                                      dep       (or (:departure-time s1) start)
;                                     d         (println dep)
                                      dep       (to-int dep)
                                      svc       (or (:service-time s1) 0)
;                                     d         (println svc)
                                      drv       (or (:driving-time s2) 0)
;                                     d         (println drv)
                                      real-eta  (safe-add dep drv)
                                      start     (when (:time_start s2) (to-int (f/parse (multiparse tz) (:time_start s2))))
                                      wait      (try (- start real-eta) (catch Exception _ 0))
                                      eta       (if (< 0 wait) start real-eta)
                                      svc       (or (:service-time s2) 0)
                                      dep       (c/to-sql-date (c/to-date-time (* 1000 (+ eta svc))))
                                      s2        (assoc s2     :real-eta       (c/to-sql-date (c/to-date-time (* 1000 real-eta)))
                                                              :waiting-time   (if (< 0 wait) wait 0)
                                                              :eta            (c/to-sql-date (c/to-date-time (* 1000 eta)))
                                                              :departure-time dep)]
                                  (conj v s2)))
                              []
                              (concat (vector (:depot p)) stops))
        stops               (if (and start traffic) (decorate-traffic start stops tz) stops)
        stops               (mapv #(into {} (filter (comp identity second) %)) (rest stops))]
   (into [] stops)))

(defn solve-route [{:keys [config depot orders terminal truck] :as p}]
  (let [depot     (assoc  (or (:depot truck) depot) :duration 0)
        terminal  (or (:terminal truck) terminal)
        stops     (filter identity (concat (vector depot) orders (vector terminal)))
        sv        (mapv :id orders)
        adj       (mapv (fn [t] (mapv #(inc (.indexOf sv (:id %))) t)) (:adjacent p))
        graph     (google-graph stops)
        config    (sol/make-ant-colony-solver-config
                    (merge
                        (select-keys config [:risk_limit :weight])
                        {:mode (keyword (:route_type config))}
                        (when (:time-window config) {:constraint :time-window})))
        cs        (filterv identity (map
                                      #(let [c (:time-window %1)] (when c (assoc c :index %2)))
                                      orders
                                      (range 1 (-> orders count inc))))
        solution  (sol/make-ant-colony-solution graph config {:time-window cs :adjacents adj})
        trials    (or (when (-> p :config :trials) (min (-> p :config :trials) ant-colony-trials)) ant-colony-trials)
        rhist     (atom [])
        ptrials   (or (-> p :config :problem-trials) 5)
        full      (loop [i 0]
                    (let [s  (ac/ant-colonies solution trials)]
                      (if
                        (every? #(= 0 %) (take 3 (butlast (:cscore s))))
                        s
                        (do
                          (when (<= i (dec ptrials))
                            (swap! rhist conj s))
                          (when (< i (dec trials))
                            (recur (inc i)))))))
        full      (or full (first (sort-by :score @rhist)))
        result    (:answer full)
        history   (:history full)
        tour      (filter identity (concat (vector (:depot result)) (:stops result) (vector (:terminal result))))
        unmet     (mapv #(assoc % :reason "time window not met" :code :window) (map-stops (map :index (-> result :unmet :unmet-windows)) (:orders p)))
        gsf       #(select-keys % [:id :meta :truck :service-time :driving-time :driving-distance
                                   :cum-dur :driving-time-in-traffic :eta-in-traffic :departure-time-in-traffic :departure-time :real-eta
                                   :real-eta-in-traffic
                                    :waiting-time :waiting-time-in-traffic :time_start :eta :time_end :lat :lng :load :load_1])
        history   (mapv #(let [r (assoc result :stops %)] (mapv (fn [o] (gsf o)) (decorate-stops r p false))) history)
        waits     (or (:waits result) (repeatedly #(identity 0)))]
    (assoc
      (select-keys p [:depot :terminal :truck :orders :adjacent])
      :adj-ndxs     adj
      :vertices     (:stops result)
      :history      history
      :score        ((:sort-fn full) full)
      :scores       (mapv #(let [s (assoc full :vertices %)] ((:sort-fn s) s)) (:history full))
      :route_start  (-> p :config :route_start)
      :stops        (mapv gsf (decorate-stops result p true))
      :unmet        unmet
      :cscore       (reduce + 0 (map #(-> % val count) (:unmet result)))
      :distance     (reduce + (:idis result))
      :duration     (+ (reduce + (:idur result)) (reduce + (:waits result)))
      :waiting      (when (:waits result) (reduce + (:waits result)))
      :lines        (when (-> p :config :lines) (mapv #(g/weight graph :points (first %) (second %)) (partition 2 1 tour))))))

(defn wrap-solve [handler]
  (fn [request]
    (let [history   (atom [])
          req       (loop [i 0]
                      (let [req       (unwrap-assign request i)
                            sols      (into [] (pmap
                                                 #(let [os     (filterv (fn [o] (= (:truck o) (:id %))) (:orders req))
                                                           oset   (into #{} (map :id os))
                                                           adj    (filterv identity (map (fn [a] (let [aset (into #{} (map :id a))] (when (superset? oset aset) a))) (:adjacent request)))
                                                           prob   (assoc (select-keys request [:config :depot :terminal]) :orders os :truck % :adjacent adj)]
                                                        (solve-route prob))
                                                  (:trucks req)))
                            score     (apply mapv + (mapv :score sols))
                            req       (assoc request :solution sols :cscore score)]
                      req))]
      (handler (or req (first (sort-by :cscore @history)))))))

;;;;;;;;;;;;;;;;;;;;;;  wrap-scenario
(defn sparse-graph [stops]
  (let [tuples      (vertices->edges (range (count stops)))
        ws          (get-augmented-weights stops tuples)]
    (g/make-concurrent-graph (count stops) ws )))

(defn compute-scenario [problem]
  (let [factor    (-> problem :trucks first :factor)
        dur-fn    #(int (Math/round (* %2 (:service-time %1))))
        orders    (mapv #(assoc % :service-time (dur-fn % factor)) (:orders problem))
        trip      (filter identity (concat (vector (:depot problem)) orders (vector (:terminal problem))))
        cs        (filterv identity (map
                                      #(let [c (:time-window %1)] (when c (assoc c :index %2)))
                                      orders
                                      (range 1 (-> orders count inc))))
        graph     (sparse-graph trip)
        vs        (range (count trip))
        r         {:idis  (intra graph :distance (vertices->edges vs))
                    :idur  (intra graph :duration (vertices->edges vs))
                    :waits (twin/wait-times graph vs cs)
                    :stops (rest vs)}
        stops     (decorate-stops r problem false)]
    (assoc (select-keys problem [:depot :terminal :trucks])
           :route_start   (-> problem :config :route_start)
           :lines         (when (-> problem :config :lines)
                            (mapv #(g/weight graph :points (first %) (second %)) (partition 2 1 vs)))
           :stops         stops
           :distance      (reduce + (:idis r))
           :duration      (+ (reduce + (:idur r)) (reduce + (:waits r))))))

(defn wrap-scenario [handler]
  (fn [request]
    (let [f       (future (core/cache-scenario (:id request) request))
          d       (log/info (:id request))
          scen    (compute-scenario request)]
      (handler (assoc request :solution scen)))))


(defn adjacent [problem]
  (let [func      (-> handler
                      wrap-cluster
                      wrap-adjacent
                      wrap-time-window
                      wrap-trucks
                      wrap-orders
                      wrap-config
                      wrap-start)]
    (func problem)))

(defn solve [problem]
  (let [history   (atom [])
        func      (-> handler
                      wrap-solve
                      wrap-cluster
                      wrap-adjacent
                      wrap-time-window
                      wrap-trucks
                      wrap-geocode
                      wrap-orders
                      wrap-config
                      wrap-start)
        trials    (or (-> problem :config :problem-trials) 5)
        sol       (loop [i 0]
                    (let [s   (func problem)]
                      (if
                        (every? #(= 0 %) (butlast (:cscore s)))
                        s
                        (do
                          (when (<= i (dec trials))
                            (swap! history conj s))
                          (when (< i (dec trials))
                            (recur (inc i)))))))]
    (or sol (first (sort-by :cscore @history)))))

(defn testit [problem]
  (let [func      (-> handler
                      wrap-time-window
                      wrap-trucks
                      wrap-geocode
                      wrap-orders
                      wrap-config
                      wrap-start)]
    (func problem)))

(defn scenario [problem]
  (let [func      (-> handler
                      wrap-scenario
                      wrap-time-window
                      wrap-trucks
                      wrap-geocode
                      wrap-orders
                      wrap-config
                      wrap-start)]
    (core/scenario-logger problem)
    (try
      (func problem)
      (catch Exception e
;       (;rintln e)
        (log/error e)
        (let [msg           (.getMessage e)]
          (assoc {} :errormessage msg :payload problem))))))

(defn histogram [problem]
  (let [func      (-> handler
                     ;(wrap-cluster)
                      (wrap-time-window)
                     ;(wrap-trucks)
                      (wrap-orders)
                      (wrap-config)
                      (wrap-start))]
    (func problem)))
