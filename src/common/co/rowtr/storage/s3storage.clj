(ns co.rowtr.storage.s3storage
  (:require
    [co.rowtr.storage         :as st    :refer [IStorage put-form fetch-form fetch-email-templates]]
    [amazonica.aws.s3         :as s3]
    [clojure.string                     :refer [split]])
  (:import
    [java.io ByteArrayInputStream]))

(def bucket-error #(ex-info "Bucket Must Be Supplied!" {}))
(def key-error    #(ex-info "Filename Must Be Supplied!" {}))



(defn store-form [{:keys [bucket key form-data]}]
  (let [bucket    (or bucket (throw bucket-error))
        key       (or key (throw key-error))
        b         (.getBytes (pr-str form-data))
        is        (ByteArrayInputStream. b)]
    (s3/put-object :bucket-name bucket
                   :key key
                   :input-stream is
                   :metadata {:content-length (count b)})))

(defn get-form  [{:keys [bucket key]}]
  (let [bucket  (or bucket (throw bucket-error))
        key     (or key (throw key-error))
        obj     (try (s3/get-object :bucket-name bucket :key key) (catch Exception _))]
    (when obj (read-string (slurp (:input-stream obj))))))

(defn get-template [{:keys [bucket key]}]
  (let [bucket  (or bucket (throw bucket-error))
        key     (or key (throw key-error))
        obj     (try (s3/get-object :bucket-name bucket :key key) (catch Exception e))]
    (when obj (slurp (:input-stream obj)))))

(defn get-html-template [{:keys [mail-type bucket] :as args}]
  (let [file    (str mail-type ".html")]
    (get-template (assoc args :key file))))

(defn get-text-template [{:keys [mail-type bucket]  :as args}]
  (let [file    (str mail-type ".txt")]
    (get-template (assoc args :key file))))

(defrecord S3StorageProvider
  [bucket]
  IStorage
  (put-form [this key form-data]
    (store-form {:bucket (:bucket this) :key key :from-data form-data}))
  (fetch-form [this key]
    (get-form {:bucket (:bucket this) :key key}))
  (fetch-email-templates [this mail-type]
    (let [html    (get-html-template {:mail-type mail-type :bucket (:bucket this)})
          text    (get-text-template {:mail-type mail-type :bucket (:bucket this)})]
      {:html html :text text})))


(defn make-s3-storage-provider [& {:keys [bucket] :as args}]
  (let [bucket    (or bucket (throw bucket-error))
        this      {:bucket  bucket}
        this      (map->S3StorageProvider this)]
    this))
