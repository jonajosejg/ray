(ns co.rowtr.tsp-api.worker
  (:require
    [clojure.stacktrace                   :as stack]
    [clojure.java.io                      :as io]
    [cemerick.bandalore                   :as sqs]
    [amazonica.aws.cloudwatch             :as cw]
    [co.rowtr.common.core                 :as core    :refer [get-geocoded-stops gfn graph-logger]]
    [co.rowtr.tsp-api.middleware          :as m       :refer [solve]]
    [clojure.java.jdbc                    :as j]
    [clojure.walk                         :as w]
    [clj-postgresql.types                 :as types]
    [clj-time.coerce                      :as timec]
    [clj-time.core                        :as t]
    [clojure.tools.logging                :as log]
    [cheshire.core                        :as json]
    [co.rowtr.common.data                 :as data]
    [co.rowtr.tsp-api.outfile             :as outfile]
    [co.rowtr.tsp-api.infile              :as infile]
    [clojure.core.async                   :as chan    :refer [>!!]]
    [co.rowtr.common.config               :as cfg     :refer [sqs-client db-conn file-out-queue cloudwatch-queue google-options]]))


(defn update-job-record
  [id upd-map]
  (j/update! db-conn :job upd-map ["idjob=?" id]))

(def job-logger   #(try
                    (let [dimensions [{:name "build" :value cfg/build}]
                          now        (timec/to-long (t/now))
                          msg-body   {:namespace "Jobs"
                                      :metric-data [{:metric-name "routing-job"
                                                      :unit "Count"
                                                      :value 1.0
                                                      :timestamp now
                                                      :dimensions dimensions}
                                                     {:metric-name "stops"
                                                      :unit   "Count"
                                                      :timestamp now
                                                      :value  (* 1.0 (-> % :problem :orders count))
                                                      :dimensions dimensions}
                                                     {:metric-name "trucks"
                                                      :unit "Count"
                                                      :timestamp now
                                                      :value  (* 1.0 (-> % :problem :trucks count))
                                                      :dimensions dimensions}]}
                          q             (sqs/create-queue sqs-client cloudwatch-queue)]
                      (sqs/send sqs-client q (pr-str msg-body)))
                    (catch Throwable t (log/error t))))

(defn process-job
  [{:keys [idjob problem idproblem idcustomer] :as data}]
  (try
    (log/info "processing job " idjob)
    (let [d   (solve problem)
          u   (apply concat (map :unmet (:solution d)))
          s   {:routes (:solution d) :elapsed-time (- (:time_end d) (:time_start d)) :unmet u}]
      (update-job-record idjob {:solution s :status "ready" :resolved (timec/to-timestamp (java.util.Date.)) :decorated d})
      (future (job-logger data))
      (when-not (nil? idproblem)
        (let [q     (sqs/create-queue sqs-client file-out-queue)]
          (sqs/send sqs-client q idjob))))
      (log/info "processed job " idjob)
      (catch Exception e
        (do
          (log/error (stack/root-cause e) " job-id " idjob)
          (update-job-record idjob {:error (.getMessage e) :status "error" :resolved (timec/to-timestamp (java.util.Date.))})))))

(defn process-input-file [id]
  (try
    (log/info "processing file " id)
    (let [r       (first (j/query db-conn ["select * from problem where idproblem=?" id]))
          d       (first (j/query db-conn ["select * from vw_depot_address where iddepot=?" (:depot r)]))
          data    (infile/fetch-from-s3 {:bucket "rowtr-problem" :key (:file r)})
          fcols   (first data)
          data    (infile/map-columns data (assoc r :depot d))
          orders  (when (:data data) (get-geocoded-stops (:data data) gfn (merge google-options {:logger graph-logger})))]
      (log/info "processed input file " id)
      (j/update! db-conn :problem (assoc (dissoc r :idproblem)
                                         :file_columns fcols
                                         :idmapping (:mapping data)
                                         :orders (when orders {:data orders})
                                         :mapped (when orders (timec/to-timestamp (java.util.Date.)))
                                         :processed (timec/to-timestamp (java.util.Date.)))
                 ["idproblem=?" id]))
    (catch Exception e
      (log/error e " problem id: " id))))

(defn process-output-file [id]
  (try
    (log/info "processing output file: " id)
    (let [d       (data/get-output-file-data id)
          wb      (outfile/make-workbook d)]
      (log/debug "workbook created")
      (outfile/stream->s3file d (outfile/workbook->stream wb)))
    (catch Throwable e
      (log/error e "job id " id))))

(defn process-message [function message]
  (let [id     (:body message)]
    (do
      (function id)
      id)))

(defn compute-job [message done-channel]
  (let [b     (:body message)]
    (if-not (= 36 (.length b))
      (let [b       (read-string b)]
        (process-job b))
      (let [id      b
            r       (first
                      (j/query
                        db-conn
                        ["select idjob, problem, idproblem, idcustomer from job where idjob=?" id]
                        :row-fn w/keywordize-keys))]
        (process-job r)))
    (>!! done-channel message)))

(defn compute-file-in [message done-channel]
  (let [b     (:body message)]
    (process-input-file b)
    (>!! done-channel message)))

(defn compute-file-out [message done-channel]
  (let [b     (:body message)]
    (process-output-file b)
    (>!! done-channel message)))

(defn compute-metric [message done-channel]
  (let [b     (:body message)]
    (cw/put-metric-data (read-string b))
    (>!! done-channel message)))
