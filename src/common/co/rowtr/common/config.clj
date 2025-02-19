(ns co.rowtr.common.config
  (:require
    [adzerk.env                 :as env]
    [cemerick.bandalore         :as sqs]))

(env/def
  JDBC_CONNECTION_STRING      nil
  ANT_COLONY_TRIALS           "10"
  IS_WORKER                   "false"
  BUILD_TYPE                  nil
  CACHE_TYPE                  nil
  SQS_ACCESS                  nil
  SQS_SECRET                  nil
  JOB_QUEUE                   "test-tsp-jobs"
  FILE_QUEUE                  "dev-tsp-files"
  FILE_IN_QUEUE               "file-in-dev"
  FILE_OUT_QUEUE              "file-out-dev"
  CLOUDWATCH_QUEUE            "cw-metrics"
  S3_ACCESS                   nil
  S3_SECRET                   nil
  S3_BUCKET                   nil
  S3_PROBLEM                  "rowtr-problem"
  S3_SOLUTION                 "rowtr-solution"
  S3_ASSETS                   "rowtr-assets"
  SEC_KEY                     nil
  GOOGLE_KEY                  nil
  GOOGLE_CLIENT               nil
  GOOGLE_SECRET               nil)

(def s3-access                    S3_ACCESS)
(def s3-secret                    S3_SECRET)
(def s3-problem-bucket            S3_PROBLEM)
(def s3-solution-bucket           S3_SOLUTION)
(def s3-asset-bucket              S3_ASSETS)
(def s3-bucket                    S3_BUCKET)
(def s3-creds                     {:access-key S3_ACCESS :secret-key S3_SECRET})

(def stripe-secret            SEC_KEY)


(def ant-colony-trials        (read-string ANT_COLONY_TRIALS))
(def worker?                  (read-string IS_WORKER))
(def build                    BUILD_TYPE)
(def sqs-access-key           SQS_ACCESS)
(def sqs-secret-key           SQS_SECRET)
(def job-queue                JOB_QUEUE)
(def file-queue               FILE_QUEUE)
(def file-in-queue            FILE_IN_QUEUE)
(def file-out-queue           FILE_OUT_QUEUE)
(def cloudwatch-queue         CLOUDWATCH_QUEUE)
(def cache-type               (keyword CACHE_TYPE))
(def db-conn                  JDBC_CONNECTION_STRING)

(def aws-creds                (if (and sqs-access-key sqs-secret-key) {:access-key sqs-access-key :secret-key sqs-secret-key} {}))

(def sqs-msg-limit            262144)
(def sqs-client               (if
                                (and sqs-access-key sqs-secret-key)
                                (sqs/create-client sqs-access-key sqs-secret-key)
                                (sqs/create-client)))

(def google-key               (when GOOGLE_KEY{:key GOOGLE_KEY}))

(def google-client            (when GOOGLE_CLIENT {:client GOOGLE_CLIENT}))

(def google-secret            (when GOOGLE_SECRET {:secret GOOGLE_SECRET}))

(def google-options           (merge google-key google-client google-secret {:method :retry}))

(def job-consumer             (atom nil))
(def file-in-consumer         (atom nil))
(def file-out-consumer        (atom nil))
(def cloudwatch-consumer      (atom nil))
(def scheduler                (atom nil))
