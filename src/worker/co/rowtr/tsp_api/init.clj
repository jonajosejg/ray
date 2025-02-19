(ns co.rowtr.tsp-api.init
  (:require
    [cemerick.bandalore                             :as sqs]
    [clojure.tools.logging                          :as log]
    [co.rowtr.tsp-api.worker                                  :refer [compute-job
                                                                      compute-file-in
                                                                      compute-metric
                                                                      compute-file-out]]
    [com.climate.squeedo.sqs-consumer               :as sq    :refer [start-consumer stop-consumer]]
    [co.rowtr.common.config                         :as cfg   :refer [sqs-client
                                                                      worker?
                                                                      job-consumer
                                                                      file-in-consumer
                                                                      file-out-consumer
                                                                      cloudwatch-consumer
                                                                      job-queue
                                                                      file-in-queue
                                                                      file-out-queue
                                                                      cloudwatch-queue]]))

(defn init-server [& ctx]
  (log/info "initializing......")
  (let [props (.stringPropertyNames (System/getProperties))]
    (log/info "properties: " (str props)))
  (try
    (when worker?
      (log/info "i'm a worker")
      (swap! file-out-consumer      (start-consumer file-out-queue      compute-file-out  :client sqs-client))
      (swap! file-in-consumer       (start-consumer file-in-queue       compute-file-in   :client sqs-client))
      (swap! cloudwatch-consumer    (start-consumer cloudwatch-queue    compute-metric    :client sqs-client))
      (swap! job-consumer           (start-consumer job-queue           compute-job       :client sqs-client)))
    (catch Throwable e
      (log/error "**************** SOME KIND OF ERROR ***************************")
      (log/error (.getMessage e))
      (log/error "**************** SOME KIND OF ERROR ***************************"))))

(defn destroy-server [& ctx]
  (when worker?
    (stop-consumer @cloudwatch-consumer)
    (stop-consumer @job-consumer)
    (stop-consumer @file-in-consumer)
    (stop-consumer @file-out-consumer)))
