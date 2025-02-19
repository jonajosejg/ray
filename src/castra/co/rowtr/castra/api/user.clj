(ns co.rowtr.castra.api.user
  (:require
    [co.rowtr.common.data           :as d   :refer [get-coords]]
    [co.rowtr.common.util           :as u   :refer [genid custom-fmt]]
    [co.rowtr.common.core                   :refer [get-geocoded-stops gfn graph-logger]]
    [co.rowtr.common.config         :as cfg :refer [s3-creds s3-access s3-secret s3-bucket file-in-queue job-queue sqs-client google-options]]
    [co.rowtr.castra.rules          :as r   :refer [allow deny logged-in? check-role owner?]]
    [co.rowtr.castra.stripe         :as stripe]
    [co.rowtr.castra.email          :as email]
    [ui.form.validation             :as v]
    [clj-time.core                  :as t   :refer [in-seconds interval]]
    [clj-time.coerce                :as c   :refer [from-sql-date from-date]]
    [clj-time.format                :as fmt]
    [adzerk.env                     :as env]
    [clojure.tools.logging          :as log]
    [clojure.set                            :refer [rename-keys]]
    [s3-beam.handler                :as s3b]
    [amazonica.aws.s3               :as s3]
    [clojure.string                         :refer [split join lower-case]]
    [cemerick.bandalore             :as sqs]
    [clj-time.coerce                :as timec]
    [digest                                 :refer [md5]]
    [castra.core                            :refer [defrpc ex *session*]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; FILE UPLOAD FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn rename-upload-file [{:keys [file-name mime-type supported-types prefix] :as args}]
  (let [suffix      (or (get supported-types mime-type) (throw (ex "Unsupported File Type" {:mime-type mime-type})))]
    (str prefix "/" (genid) suffix)))

(defn sign-file [{:keys [file-name mime-type signing-data supported-types] :as args}]
  (let [new-file    (rename-upload-file args)
        mdata       {:file-name new-file :mime-type mime-type}
        signature   (s3b/sign-upload mdata  signing-data)]
    (assoc signature :original-file-name (:file-name args))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; STATE FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-state* []
  (let [user        (-> @*session* :user)
        state       (when user (d/get-user-state user))]
    state))

(defrpc get-state []
  {:rpc/pre [(allow)]
   :rpc/query (get-state*)}
  nil)

(defrpc update-capacity-config [args]
  {:rpc/pre [(logged-in?)]
   :rpc/query (get-state*)}
  (let [creds (select-keys (:user @*session*) [:iduser :idcustomer])
        data  (filterv identity (mapv val args))]
    (d/update-capacity (merge {:cap_config data} creds))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; LOGIN FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrpc verify! [args]
  {:rpc/pre [(allow)]}
  (let [res       (d/verify! args)]
    (if (= res :success) :ok (throw (ex "Verification Hash Not Found" {})))))

(defrpc signup! [{:keys [login passwd1 passwd2] :as args}]
  {:rpc/pre [(allow)
             (let [v1   (v/validate args v/required [:customername :street :city :state :postalcode :phone
                                                     :firstname :lastname :login :passwd1 :passwd2])
                   v2   (when-not (= passwd1 passwd2) {:passwd2 "Passwords must match"})
                   v3   (when-not (= :ok (d/check-email login)) {:login "Email already assigned."})
                   v    (merge v1 v2 v3)]
               (when (seq v) (throw (ex "Validation Failed" v))))]}
   (let [customer   (d/add-customer (select-keys args [:customername :street :suite :city :state :postalcode :code :phone]))
         login      (lower-case login)
         passwd     (md5 (str login passwd1))
         user       (d/add-user (merge
                                  (select-keys args [:firstname :lastname])
                                  {:verified false :vrhash (genid) :can_login false}
                                  {:passwd passwd :login login}
                                  (select-keys customer [:idcustomer])))]
     (email/send-verification-email {:recipient login :vrhash (:vrhash user)})
     :ok))

(defrpc login!  [{:keys [user pass] :as login}]
    {:rpc/pre [(let [v (v/validate login v/required [:user :pass])]
                 (when (seq v) (throw (ex "Validation Failed" v)))) ]
   :rpc/query (get-state*)}
  (r/login! user pass))

(defrpc request-password-reset [{:keys [user]}]
  {:rpc/pre [(allow) (when (nil? user) (throw (ex "Validation Failed" {:user "This field is required"})))]}
  (let [lphash        (genid)
        user          (d/get-user-by-email user)]
    (if user
      (do
        (d/update-user (assoc user :lphash lphash))
        (email/send-password-reset-email {:recipient (:login user) :lphash lphash})
        :ok)
      (throw (ex "Validation Failure" {:email "Email not Found"})))))

(defrpc password-reset [{:keys [lphash passwd1 passwd2]}]
  {:rpc/pre [(allow) (when (not= passwd1 passwd2) (throw (ex "Validation Failed" {:passwd1 "Passwords must match"})))]}
  (let [user          (d/get-user-by-lphash lphash)]
    (if user
      (let [passwd      (md5 (str (:login user) passwd1))]
        (d/update-user (assoc user :passwd passwd :lphash nil))
        :ok)
      (throw (ex "Validation Failed - Hash Not Found" {})))))

(defrpc change-password [{:keys [login oldpassword newpass1 newpass2] :as args}]
  {:rpc/pre [(logged-in?)
             (let [v (v/validate args v/required [:login :oldpassword :newpass1 :newpass2])]
                (when (seq v) (throw (ex "Validation Failed" v))))
             (when (not= newpass1 newpass2) (throw (ex "Validation Failed" {:newpass2 "Passwords must match."})))
             (let [hash   (md5 (str login oldpassword))]
               (when-not (d/validate-login login hash) (throw (ex "Validation Failed" {:oldpassword "Password is incorrect"}))))]}

  (let [creds         (select-keys (:user @*session*) [:iduser :idcustomer])
        hash          (md5 (str login newpass1))]
   (d/update-user (merge creds {:passwd hash}))))

(defrpc logout! []
  {:rpc/pre [(r/logout!)]
   :rpc/query (get-state*)}
  nil)

(defrpc get-job [{:keys [id idcustomer]}]
  {:rpc/pre [(logged-in?) (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]}
  (let [rec         (d/get-user-job id)]
    (dissoc rec :decorated)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; ENTITY FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; T R U C K ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrpc add-truck [truck]
  {:rpc/pre [(logged-in?)
             (let [v  (v/validate truck v/required [:iddepot :label :capacity])]
               (when (seq v) (throw (ex "Validation Failed" v))))]
   :rpc/query (get-state*)}
  (let [creds             (select-keys (:user @*session*) [:iduser :idcustomer])]
    (d/add-truck (merge truck creds))))

(defrpc update-truck [truck]
  {:rpc/pre [(logged-in?) (owner? {:entity :truck :eid (:idtruck truck)})
             (let [v  (v/validate truck v/required [:iddepot :label :capacity])]
               (when (seq v) (throw (ex "Validation Failed" v))))]
   :rpc/query (get-state*)}
  (let [creds             (select-keys (:user @*session*) [:iduser :idcustomer])]
    (d/update-truck (merge truck creds))))

(defrpc delete-truck [truck]
  {:rpc/pre [(logged-in?) (owner? {:entity :truck :eid (:idtruck truck)})]
   :rpc/query (get-state*)}
  (d/delete-truck truck))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; D E P O T ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrpc add-depot [{:keys [street city state postalcode] :as depot}]
  {:rpc/pre [(logged-in?) ]
   :rpc/query (get-state*)}
  (let [creds             (select-keys (:user @*session*) [:iduser :idcustomer])
        coords            (get-coords (join ", " [street city state postalcode]))
        tz                {:tz_id (d/get-tzid coords)}]
    (d/add-depot (merge depot creds coords tz))))


(defrpc update-depot [{:keys [iddepot label street city state postalcode lat lng] :as depot}]
  {:rpc/pre [(logged-in?)(owner? {:entity :depot :eid (:iddepot depot)})]
   :rpc/query (get-state*)}
  (let [creds             (select-keys (:user @*session*) [:iduser :idcustomer])
        coords            (get-coords (join ", " [street city state postalcode]))]
    (d/update-depot (merge depot creds coords))))

(defrpc delete-depot [depot]
  {:rpc/pre [(logged-in?)(owner? {:entity :depot :eid (:iddepot depot)})]
   :rpc/query (get-state*)}
  (d/delete-depot depot))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; P R O B L E M ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def problem-mime-types   {"text/csv"                   ".csv"
                           "application/vnd.ms-excel"   ".csv"})


(def problem-upload-data  {:bucket           cfg/s3-problem-bucket
                          :aws-zone         "us-east-1"
                          :aws-access-key   cfg/s3-access
                          :aws-secret-key   cfg/s3-secret
                          :acl              "private"})

(defrpc get-problem [id]
  {:rpc/pre [(logged-in?)]}
  (let [p       (d/get-problem id)]
    (assoc p :column_map (reduce (fn [m [k v]] (assoc m k (keyword v))) {} (:column_map p)))))

(defrpc add-problem-mapping [{:keys [mapping idproblem]}]
  {:rpc/pre         [(logged-in?)]
   :rpc/query       (when idproblem (d/get-problem idproblem))}
  (let [creds             (select-keys (:user @*session*) [:idcustomer])]
    (d/add-mapping (merge creds {:column_map mapping}))
    (when idproblem
      (d/update-problem {:command :add-mapping-time :idproblem idproblem :operand (timec/to-timestamp (java.util.Date.))})
      (let [q             (sqs/create-queue sqs-client file-in-queue)]
        (sqs/send sqs-client q idproblem)))))

(defrpc add-problem [{:keys [idproblem] :as problem}]
  {:rpc/pre [(logged-in?)]
   :rpc/query (get-state*)}
  (let [creds             (select-keys (:user @*session*) [:iduser :idcustomer])
        q                 (sqs/create-queue sqs-client file-in-queue)
        idproblem         (d/add-problem (merge problem creds ))]
     (sqs/send sqs-client q idproblem)))

(defrpc sign-problem [{:keys [file-name mime-type meta] :as payload}]
  {:rpc/pre [(logged-in?)]}
  (let [customer    (or (-> @*session* :user :idcustomer) "unknown")
        prefix      (str "problems/" customer)
        sig         (sign-file (merge payload {:prefix prefix
                                               :supported-types problem-mime-types
                                               :signing-data problem-upload-data}))]
    {:signature sig :meta meta}))

(defrpc update-address [{:keys [idcustomer idproblem id address] :as args}]
  {:rpc/pre [(logged-in?)
             (let  [v  (v/validate args v/required  [:idproblem :idcustomer])]
                (when  (seq v)  (throw  (ex  "Validation Failed" v))))
             (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]
   :rpc/query (d/get-problem idproblem)}
  (let [problem       (d/get-problem idproblem)
        data          (-> problem :orders :data)
        item          (first (filter #(= (:id %) id) data))
        ndx           (.indexOf data item)
        item          (assoc (merge item (select-keys args (into [] (keys item)))) :address address :lat nil :lng nil)
        data          (assoc-in data [ndx] item)
        orders        (get-geocoded-stops data gfn (merge google-options {:logger graph-logger}))]
    (d/update-problem {:idproblem idproblem :operand {:data orders} :command :replace-orders})))

(defrpc exclude-address [{:keys [idcustomer idproblem id] :as args}]
  {:rpc/pre [(logged-in?)
             (let  [v  (v/validate args v/required  [:idproblem :idcustomer])]
                (when  (seq v)  (throw  (ex  "Validation Failed" v))))
             (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]
   :rpc/query (d/get-problem idproblem)}
  (let [problem       (d/get-problem idproblem)
        data          (-> problem :orders :data)
        data          (mapv #(if (= (:id %) id) (assoc % :excluded true) %) data)]
    (d/update-problem {:idproblem idproblem :operand {:data data} :command :replace-orders})))

(defrpc restore-address [{:keys [idcustomer idproblem id] :as args}]
  {:rpc/pre [(logged-in?)
             (let  [v  (v/validate args v/required  [:idproblem :idcustomer])]
                (when  (seq v)  (throw  (ex  "Validation Failed" v))))
             (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]
   :rpc/query (d/get-problem idproblem)}
  (let [problem       (d/get-problem idproblem)
        data          (-> problem :orders :data)
        data          (mapv #(if (= (:id %) id) (dissoc % :excluded) %) data)]
    (d/update-problem {:idproblem idproblem :operand {:data data} :command :replace-orders})))

(defrpc clear-all-excluded [{:keys [idcustomer idproblem id] :as args}]
  {:rpc/pre [(logged-in?)
             (let  [v  (v/validate args v/required  [:idproblem :idcustomer])]
                (when  (seq v)  (throw  (ex  "Validation Failed" v))))
             (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]
   :rpc/query (d/get-problem idproblem)}
  (let [problem       (d/get-problem idproblem)
        data          (-> problem :orders :data)
        data          (mapv #(dissoc % :excluded) data)]
    (d/update-problem {:idproblem idproblem :operand {:data data} :command :replace-orders})))

(defrpc update-problem [{:keys [idcustomer idproblem command] :as args}]
  {:rpc/pre [(logged-in?) (owner? {:entity :problem :eid idproblem})]
   :rpc/query (d/get-problem idproblem)}
  (let [creds         (select-keys (:user @*session*) [:iduser :idcustomer]) ]
    (d/update-problem (merge args creds))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;; COLLECTION FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-job-files [idcustomer idproblem]
  (let [obs       (:object-summaries (s3/list-objects s3-creds
                                                      :bucket-name "rowtr-solution"
                                                      :prefix (str "solutions/" idcustomer "/" idproblem "/")))]
    (mapv :key obs)))


(defrpc get-problems [args]
  {:rpc/pre [(logged-in?)]}
  (let [creds     (select-keys (-> @*session* :user) [:iduser :idcustomer])
        cnt       (d/get-problem-count (merge args creds))
        probs     (d/get-recent-problems (merge args creds))]
    {:count (:cnt cnt) :problems probs}))

(defrpc get-jobs [args]
  {:rpc/pre [(logged-in?)
             (let [v    (v/validate args v/required [:idproblem])]
               (when (seq v) (throw (ex "Validation Failed" v))))]}
  (let [creds     (select-keys (-> @*session* :user) [:iduser :idcustomer])
        cnt       (d/get-problem-job-count (:idproblem args))
        files     (get-job-files (:idcustomer creds) (:idproblem args))
        jobs      (mapv #(let [f (first (filter (fn [file] (re-find (re-pattern (:idjob %)) file)) files))]
                           (assoc % :file f)) (d/get-problem-jobs args))
        res       {:count (:cnt cnt) :jobs jobs}]
    res))

(defn- recheck-job-files [jobs]
  (let [q     (sqs/create-queue sqs-client cfg/file-out-queue)]
    (doseq [job jobs :when (nil? (:file job))]
      (let [diff    (try
                      (in-seconds
                        (interval
                          (from-sql-date (:resolved job))
                          (from-date (java.util.Date.))))
                      (catch Throwable t 0))]
        (when (<= 30 diff) (sqs/send sqs-client q (:idjob job)))))))

(defrpc get-client-jobs [{:keys [idcustomer idproblem] :as args}]
  {:rpc/pre [(logged-in?)
             (let  [v  (v/validate args v/required  [:idcustomer :idproblem])]
                (when  (seq v)  (throw  (ex  "Validation Failed" v))))
             (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]}
  (let [cnt       (d/get-job-count idcustomer idproblem)
        files     (get-job-files idcustomer idproblem)
        jobs      (mapv
                    #(assoc % :file (when (contains? files (:idjob %)) true))
                    (d/get-customer-jobs args))]
    (recheck-job-files jobs)
    {:count (:cnt cnt) :jobs jobs}))

(defrpc clear-process [{:keys [idproblem idcustomer] :as args}]
  {:rpc/pre   [(logged-in?) (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]
   :rpc/query (d/get-problem idproblem)}
  (d/update-problem {:idproblem idproblem :reduced nil :trucks nil}))

(defrpc submit-job [{:keys [idproblem idcustomer] :as args}]
  {:rpc/pre     [(logged-in?) (r/valid-role? [(r/admin?) (r/customer? idcustomer)])]}
  (let [start       (try
                      (when (:route_start args) (fmt/parse custom-fmt (:route_start args)))
                      (catch Exception e (throw (ex "Validation Failed" {:route_start "Invalid DateTime Format"}))))
        idcustomer  (or idcustomer (-> @*session* :user :idcustomer))
        q           (sqs/create-queue sqs-client job-queue)
        problem     (d/get-problem idproblem)
        depot       (let [d   (d/get-depot (:depot problem))] (assoc d :id (or (:code d) (:label d))))
        trucks      (mapv #(let [t (d/get-truck %)]
                             (assoc t
                                    :id (or (:code t) (:label t))
                                    :capacity (first (:capacity t))
                                    :capacity_1 (second (:capacity t))))
                          (:trucks problem))
        config      (merge (:config problem) (when start {:route_start (fmt/unparse (fmt/formatters :date-hour-minute-second) start)}))
        orders      (filterv #(and (not (:excluded %)) (not= nil (:lat %))) (-> problem :orders :data))
        orders      (if-not
                      start
                      orders
                      (let [dt    (fmt/unparse (fmt/formatter "MM/dd/YYYY") start)
                            tzid  (:tz_id depot)
                            tz    (t/time-zone-for-id tzid)]
                        (mapv #(do
                                 (assoc
                                        %
                                        :time_start  (when (:time_start %) (u/make-date-from-parts dt (fmt/unparse-local u/time-only (c/to-local-date-time (fmt/parse (u/multiparse tz) (:time_start %)))) tzid))
                                        :time_end    (when (:time_end %) (u/make-date-from-parts dt (fmt/unparse-local u/time-only (c/to-local-date-time (fmt/parse (u/multiparse tz) (:time_end %)))) tzid))))
                              orders)))
        p           (assoc {} :config (merge config {:lines true}) :depot depot :orders orders :trucks trucks)
        idjob       (d/add-job idproblem idcustomer p)]
    (sqs/send sqs-client q idjob)
    (get-jobs args)))

(defrpc update-billing [{:keys [idcustomer source card] :as args}]
  {:rpc/pre     [(logged-in?)
                 (let [v  (v/validate args v/required [:idcustomer])]
                   (when (seq v) (throw (ex "Validation Failed" v))))]}
  (let [cust    (d/get-customer idcustomer)
        email   (:login (d/get-user-by-id (-> @*session* :user :iduser)))
        newcust (if-not (:customer_data cust)
                  (stripe/new-customer {:secret-key cfg/stripe-secret
                                          :email email
                                          :description (:customername cust)
                                          :card card
                                          :source (:id source)})
                  (let [cust    (-> cust :customer_data :id)]
                    (stripe/update-customer-source {:secret-key cfg/stripe-secret
                                                    :customer cust
                                                    :card card
                                                    :source (:id source)})))]
    (d/update-source {:idcustomer idcustomer :customer newcust})))
