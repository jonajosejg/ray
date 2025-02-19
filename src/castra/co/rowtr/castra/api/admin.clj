(ns co.rowtr.castra.api.admin
  (:require
    [co.rowtr.common.data           :as d   :refer [get-coords]]
    [co.rowtr.common.util           :as u   :refer [genid]]
    [co.rowtr.common.config         :as cfg :refer [s3-creds s3-access s3-secret s3-bucket file-in-queue job-queue sqs-client google-options]]
    [co.rowtr.castra.rules          :as r   :refer [allow deny logged-in? check-role]]
    [ui.form.validation             :as v]
    [clj-time.core                  :as t   :refer [in-seconds interval]]
    [clj-time.coerce                :as c   :refer [from-sql-date from-date]]
    [clojure.tools.logging          :as log]
    [clojure.string                         :refer [split join]]
    [digest                                 :refer [md5]]
    [google-maps-web-api.core               :refer [google-geocode]]
    [castra.core                            :refer [defrpc ex *session*]]))

(defn get-state* []
  (let [user        (-> @*session* :user)
        state       (when user (d/get-admin-state user))]
    state))

(defrpc get-state []
  {:rpc/pre [(allow)]
   :rpc/query (get-state*)}
  nil)

(defrpc add-update-customer [{:keys [street city state postalcode] :as customer}]
  {:rpc/pre [(logged-in?) (r/valid-role? [(r/admin?)])]
   :rpc/query (get-state*)}
  (let [coords            (get-coords (join ", " [street city state postalcode]))
        obj               (merge coords customer)]
    (if
      (:idcustomer obj)
      (d/update-customer obj)
      (d/add-customer obj))))

(defrpc add-update-user [{:keys [login passwd iduser] :as user-obj}]
  {:rpc/pre [(logged-in?) (r/valid-role? [(r/admin?)])]
   :rpc/query (get-state*)}
  (let [pass        (when (and login passwd) (md5 (str login passwd)))
        user        (if pass (assoc user-obj :passwd pass) user-obj)]
    (if
      iduser
      (d/update-user user)
      (d/add-user user))))

(defrpc login!  [{:keys [user pass] :as login}]
    {:rpc/pre [(let [v (v/validate login v/required [:user :pass])]
                 (when (seq v) (throw (ex "Validation Failed" v)))) ]
   :rpc/query (get-state*)}
  (r/login! user pass :admin))

(defrpc impersonate!  [{:keys [target user pass] :as login}]
    {:rpc/pre [(let [v (v/validate login v/required [:user :pass])]
                 (when (seq v) (throw (ex "Validation Failed" v)))) ]
   :rpc/query (get-state*)}
  (r/act-as-user target user pass))

(defrpc get-jobs [{:keys [idcustomer] :as args}]
  {:rpc/pre [(logged-in?) (r/valid-role? [(r/admin?)])]}
  (let [cnt           (d/get-customer-job-count idcustomer)
        jobs          (d/get-customer-jobs args)]
    {:count (:cnt cnt) :jobs jobs}))
