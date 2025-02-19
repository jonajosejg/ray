(ns co.rowtr.tsp-api.api
  (:require
    [liberator.core                                   :refer [resource defresource]]
    [clojure.java.io                        :as io]
    [adzerk.env                             :as env]
    [clojure.tools.logging                  :as log]
    [cemerick.bandalore                     :as sqs]
    [cheshire.core                          :as json]
    [clojure.java.jdbc                      :as j]
    [clj-postgresql.types                   :as types]
    [clj-time.coerce                        :as timec]
    [digest                                           :refer [md5]]
    [co.rowtr.tsp-api.middleware            :as m]
    [co.rowtr.common.data                   :as d]
    [co.rowtr.common.util                   :as u     :refer [prepare-solution-for-rest prepare-scenario-for-rest]]
    [co.rowtr.common.config                           :refer [sqs-client
                                                              db-conn
                                                              build
                                                              sqs-msg-limit
                                                              job-queue]]
    [compojure.core                         :refer [defroutes ANY]]))

(defn- body-as-string [ctx]
  (if-let [body   (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

(defn- parse-json [ctx key]
  (when ( #{:put :post} (get-in ctx [:request :request-method]))
    (try
      (if-let [body     (body-as-string ctx)]
          (let [data    (json/parse-string body true)]
          [false {key data}])
        {:message "No Body"})
      (catch Exception e
       ;(.printStackTrace e)
        {:message (format "IOException: %s" (.getMessage e))}))))

(defn- check-content-type [ctx content-types]
  (if (#{:put :post} (get-in ctx [:request :request-method]))
    (or
      (some #{(get-in ctx [:request :headers "content-type"])}
            content-types)
      [false {:message "Unsuported Content-Type"}])
    true))

(defresource route
  :available-media-types  ["application/json"]
  :allowed-methods        [:post]
  :authorized?            #(contains? #{:user :admin} (-> % :request :basic-authentication :role))
  :known-content-type?    #(check-content-type %  ["application/json"])
  :malformed?             #(parse-json % ::data)
  :post!                  #(let [id       (str (java.util.UUID/randomUUID))
                                 problem  (::data %)
                                 job-queue  (sqs/create-queue sqs-client job-queue)
                                 cust     (-> % :request :basic-authentication :cust) ]
                            (j/insert!  db-conn :job
                              (assoc
                                {}
                                :idjob      id
                                :problem    problem
                                :metadata   {:build build}
                                :submitted  (timec/to-timestamp (java.util.Date.))
                                :idcustomer cust
                                :status     "pending"))
                            (log/info "posting to " job-queue)
                            (let [pstr    (pr-str {:idjob id :problem problem :idcustomer cust})]
                              (if (< sqs-msg-limit (.length pstr))
                                (sqs/send sqs-client job-queue id)
                                (sqs/send sqs-client job-queue pstr)))

                            {::id id})
  :post-redirect?         false
  :new?                   true
  :handle-created         #(do
                             (log/info "created job " (::id %))
                             (assoc {} :id  (::id %))))

(defresource scenario
  :available-media-types  ["application/json"]
  :allowed-methods        [:post]
  :authorized?            #(contains? #{:user :admin} (-> % :request :basic-authentication :role))
  :known-content-type?    #(check-content-type %  ["application/json"])
  :malformed?             #(parse-json % ::data)
  :post!                  #(let [id           (str (java.util.UUID/randomUUID))
                                 problem      (::data %)
                                  res         (m/scenario (assoc problem :id id))
                                  res         (assoc-in res [:solution :elapsed-time] (- (:time_end res) (:time_start res)))]
                             {::result (assoc-in  res [:solution :id] id)})
  :post-redirect?         false
  :new?                   false
  :respond-with-entity?   true
  :handle-ok              #(do
                            (log/info "processed   scenario .........")
                            (let [res       (prepare-scenario-for-rest (::result %))]
                              (select-keys res [:solution]))))


(def status-handle-ok   (fn [ctx]
                           (let  [stat    (keyword (-> ctx ::data :status))
                                  data    (::data ctx)
                                  fn-map  { :pending #(select-keys data [:status])
                                            :ready   #(select-keys (prepare-solution-for-rest data) [:status :solution])
                                            :error   #(select-keys data [:status :error])}]
                             ((stat fn-map)))))

(defresource status [id]
  :available-media-types  ["application/json"]
  :allowed-methods        [:get]
  :authorized?            #(contains? #{:user :admin} (-> % :request :basic-authentication :role))
  :exists?                 (fn [ctx]
                            (let [cust    (-> ctx :request :basic-authentication :cust)
                                  data    (d/get-user-job id) ]
                              (if (or (= (-> ctx :request :basic-authentication :role) :admin) (= cust (:idcustomer data)))
                                [true {::data data}]
                                false)))
  :handle-ok              status-handle-ok)

(defresource health
  :available-media-types  ["application/json"]
  :allowed-methods        [:get]
  :handle-ok              (fn [ctx]
                            (let [job-queue   (sqs/create-queue sqs-client job-queue)]
  ;                          (log/info "checking health")
                             (clojure.walk/keywordize-keys (sqs/queue-attrs sqs-client job-queue)))))


(defroutes app-routes
  (ANY "/foo" [] (resource :available-media-types ["text/html"]
                           :handle-ok "<html>Hello, Internet.</html>"))
  (ANY "/route" [] route)
  (ANY "/scenario" [] scenario)
  (ANY "/status/:id" [id] (status id))
  (ANY "/heartbeat" [] health))


;(defonce open-routes #{"/foo" "/heartbeat" "/test"})
 (defonce open-routes #{"/foo" "/heartbeat"})

(defmulti check-user-credentials (fn [which _ _] which) :default :dynamo)

(defmethod check-user-credentials :postgres
  [_ name password]
  (let [hash        (md5 (str name password))
        q           "select * from login where login=?"
        result      (first (j/query db-conn [q name]))]
    (when (= (:passwd result) hash)
      (assoc
        {}
        :login name
        :cust (:idcustomer result)
        :role (or (keyword (:role result)) :user)))))

(defn authenticated? [name password request]
  (if
    (contains? open-routes (:uri request))
    true
    (check-user-credentials :postgres name password)))

