(ns co.rowtr.common.core
  (:require
    [amazonica.aws.cloudwatch               :as cw]
    [clojure.tools.logging                  :as log]
    [cemerick.bandalore                     :as sqs]
    [co.rowtr.common.config                 :as cfg     :refer [cache-type sqs-client cloudwatch-queue]]
    [co.rowtr.geo-cache.redis-cache         :as rc]
    [clj-time.coerce                                    :refer [to-long]]
    [clj-time.core                          :as t]
    [co.rowtr.geo-cache.cache               :as ca      :refer [memoize-geocode memoize-weight]]
    [co.rowtr.geo-graph.google              :as graph]))

(def logger           (fn [ns metric]
                        (try
                          (let [now          (to-long (t/now))
                                msg-body     {:namespace ns
                                              :metric-data [{:metric-name (name metric)
                                                              :unit "Count"
                                                              :timestamp now
                                                              :value 1.0}]}
                                q            (sqs/create-queue sqs-client cloudwatch-queue)]
                            (sqs/send sqs-client q (pr-str msg-body)))
                         (catch Throwable t (log/error t)))))

(def scenario-logger   #(try
                          (let [dimensions [{:name "build" :value cfg/build}]
                                now        (to-long (t/now))
                                msg-body   {:namespace "Scenario"
                                            :metric-data [{:metric-name "scenario"
                                                            :unit "Count"
                                                            :value 1.0
                                                            :timestamp now
                                                            :dimensions dimensions}
                                                          {:metric-name "stops"
                                                            :unit   "Count"
                                                            :timestamp now
                                                            :value  (* 1.0 (-> % :orders count))
                                                            :dimensions dimensions}
                                                          {:metric-name "trucks"
                                                            :unit "Count"
                                                            :timestamp now
                                                            :value  (* 1.0 (-> % :trucks count))
                                                            :dimensions dimensions}]}
                                q             (sqs/create-queue sqs-client cloudwatch-queue)]
                            (sqs/send sqs-client q (pr-str msg-body)))
                    (catch Throwable t (log/error t))))

(def cache-logger     (partial logger "GeoCache"))
(def graph-logger     (partial logger "GeoGraph"))

(def cache            (when (= cfg/cache-type :redis) (ca/get-cache {:type :redis :logger cache-logger})))

(defn cache-scenario [id scenario]
  (rc/cache-scenario id scenario))

(defn get-scenario [id]
  (rc/get-scenario id))

;;;;;;;;;;;;;;;;; helper functions
(def geocode          graph/address->latlon)
(def geocode-cached   #(memoize-geocode % graph/address->latlon))
(def gfn              (if cache (geocode-cached cache)  geocode))

(def weight           graph/get-weight)
(def weight-cached    #(memoize-weight  % graph/get-weight))
(def wfn              (if cache (weight-cached cache)  weight))

(defn get-geocoded-stops [stops gfn options]
  (mapv
    #(if-not
      (and (:lat %) (:lng %))
      (let [coords    (try (gfn (merge (select-keys % [:address]) options)) (catch Throwable e {}))]
        (merge % (select-keys coords [:lat :lng])))
      %)
    stops))
