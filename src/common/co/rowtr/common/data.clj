(ns co.rowtr.common.data
  (:require
    [adzerk.env             :as env]
    [clj-postgresql.types   :as type]
    [clojure.tools.logging  :as log]
    [clojure.set                            :refer [difference]]
    [clojure.walk           :as w           :refer [keywordize-keys]]
    [co.rowtr.common.util   :as u           :refer [genid]]
    [co.rowtr.common.config :as cfg         :refer [db-conn]]
    [google-maps-web-api.core               :refer [google-geocode google-timezone]]
    [clojure.java.jdbc      :as j]))

(def get-coords     #(-> (google-geocode {:address %}) :results first :geometry :location))
(def get-tzid       #(-> (google-timezone {:location (select-keys % [:lat :lng])}) :timeZoneId))

(defn validate-login [email login-hash & [role]]
  (let [role          (or role :user)
        q             (str "select * from login where login = ? and can_login = true" (when (= role :admin) " and role='admin'"))
        result        (first (j/query db-conn [q email]))]
    (when (= (:passwd result) login-hash) result)))

(defn verify! [{:keys [vrhash]}]
  (let [q     ["select * from login where vrhash=?" vrhash]
        rec   (first (j/query db-conn q))]
    (when rec
      (j/update! db-conn :login {:can_login true :verified true :vrhash nil} ["iduser=?" (:iduser rec)])
      :success)))

(defn get-mappings
  ([]
    (j/query db-conn ["select * from problem_columns"] :row-fn keywordize-keys))
  ([idcustomer]
    (let [sql "select * from mapping where idcustomer=?"]
      (j/query db-conn [sql idcustomer] :row-fn keywordize-keys))))

(defn get-output-file-data [id]
  (let [q       "select j.idjob,j.idcustomer, p.file_columns, j.solution, j.idproblem, c.cap_config
                from job j
                join problem p using (idproblem)
                join customer c on j.idcustomer=c.idcustomer
                where j.idjob=?"]
    (first (j/query db-conn [q id] :row-fn keywordize-keys))))

(defn log-trx-email [{:keys [id recipient template data] :as args}]
  (j/insert! db-conn :trx_emails args))


;;;;;;;;;;;;;;;;;;;;;;; JOBS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-client-jobs-for-period [{:keys [client start-date end-date]}]
  (let [q       "select idjob, problem from vw_good_jobs where idcustomer = ? and resolved between ? and ?"
        s       (java.sql.Date/valueOf start-date)
        e       (java.sql.Date/valueOf end-date)]
    (j/query db-conn [q client s e] :row-fn keywordize-keys)))

(defmulti get-billing-data-for-period :bill-type :default :per-month)

(defmethod get-billing-data-for-period :per-month [{:keys [client start-date end-date] :as args}]
  (let [jobs    (get-client-jobs-for-period args)
        trucks  (flatten (map #(-> % :problem :trucks) jobs))]
    (-> (map :id trucks) frequencies)))

(defmethod get-billing-data-for-period :per-shift [{:keys [client start-date end-date] :as args}]
  (let [jobs    (get-client-jobs-for-period args)
        gjobs   (group-by #(-> % :problem :config :route_start) jobs)
        counts  (map (fn [j] (assoc
                               {}
                               :shift_start (first j)
                               :count (count (distinct (mapv :id (flatten (map (fn [p] (-> p :problem :trucks)) (second j))))))))
                     gjobs)]
   (reverse (sort-by :count counts))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; ENTITY FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-entity-by-id [{:keys [entity eid]}]
  (let [q         (str "select iduser from " (name entity) " where " (str "id" (name entity)) "=?")]
    (first (j/query db-conn [q eid]))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; A D D R E S S ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def address-fields [:street :suite :city :state :postalcode :lat :lng :tz_id])

(defn add-address [obj]
  (let [idaddress     (genid)]
    (j/insert!  db-conn :address (assoc (select-keys obj address-fields) :idaddress idaddress))
    idaddress))

(defn same-address? [o n]
  (= (select-keys o address-fields) (select-keys n address-fields)))

(defn get-address-id [o n]
 (if
  (same-address? o n)
  (:idaddress o)
  (add-address n)))

(defn add-mapping [{:keys [] :as args}]
  (let [idmapping   (genid)
        data        (assoc args :idmapping idmapping)]
    (j/insert! db-conn :mapping data)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; T R U C K ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-truck [id]
  (first (j/query db-conn ["select * from truck where idtruck=?" id] :row-fn keywordize-keys)))

(defn add-truck [obj]
  (let [cap           (try
                        (mapv #(java.lang.Integer/parseInt (str %)) (:capacity obj))
                        (catch Exception e
                          (log/error (.getMessage e))
                          [0]))
        truck         (assoc obj :capacity cap :idtruck (genid))]
    (j/insert! db-conn :truck truck)))

(defn update-truck [obj]
  (let [cap           (try
                        (mapv #(java.lang.Integer/parseInt (str %)) (:capacity obj))
                        (catch Exception e
                          (log/error (.getMessage e))
                          0))]
    (j/update! db-conn :truck (assoc obj :capacity cap) ["idtruck=?" (:idtruck obj)])))

(defn delete-truck [obj]
  (j/update! db-conn :truck {:deleted true} ["idtruck=?" (:idtruck obj)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; D E P O T ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-depot [id]
  (first (j/query db-conn
                  ["select * from vw_depot_address where iddepot=?" id]
                  :row-fn keywordize-keys)))

(defn add-depot [obj]
  (let [depot         (assoc (apply dissoc obj address-fields) :iddepot (genid))
        address       (assoc (select-keys obj address-fields) :idaddress (genid))]
    (try
      (j/with-db-transaction [t-con db-conn]
        (j/insert! t-con :address address)
        (j/insert! t-con :depot (assoc depot :idaddress (:idaddress address))))
      (catch Exception e
        (log/error (.getMessage e))))))

(defn update-depot [{:keys [iddepot label iduser] :as obj} ]
  (let [old         (first (j/query db-conn ["select * from address where idaddress=?" (:idaddress obj)]))
        idaddress   (get-address-id old obj)
        depot       (assoc (apply dissoc obj address-fields) :idaddress idaddress)]
    (j/update! db-conn :depot depot ["iddepot=?" iddepot])))

(defn delete-depot [obj]
  (j/update! db-conn :depot {:deleted true} ["iddepot=?" (:iddepot obj)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; F I L E ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; J O B ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-user-job [id]
  (keywordize-keys (first (j/query db-conn ["select idjob, idcustomer, idproblem, status, problem, solution, submitted, resolved, decorated  from job where idjob=?" id]))))

(defn add-job [idproblem idcustomer problem]
  (let [id          (genid)]
    (j/insert! db-conn :job {:idjob id :idproblem idproblem :idcustomer idcustomer :problem problem :status "pending" :metadata {:build cfg/build}})
    id))

;;;;;TODO remove this funtion for the three below it
(defn get-job-count
  ([client]
   (get-job-count client nil))
  ([client problem]
    (let [client        (or client "%")
          problem       (or problem "%")
          sql           "select count(*) cnt from job where status='ready' and idcustomer like ? and idproblem like ?"]
      (first (j/query db-conn [sql client problem])))))

(defn get-customer-job-count [idcustomer]
  (let [idcustomer      (or idcustomer "%")
        sql             "select count(*) cnt from job where status='ready' and idcustomer like ?"]
    (first (j/query db-conn [sql idcustomer]))))

(defn get-problem-job-count [idproblem]
  (let [sql             "select count(*) cnt from job where status in ('ready','pending') and idproblem like ?"]
    (first (j/query db-conn [sql idproblem]))))

(defn get-user-job-count [iduser]
  (let [sql             "select count(*) cnt from job where status='ready' and iduser like ?"]
    (first (j/query db-conn [sql iduser]))))


(defn get-recent-jobs
  ([client]
   (get-recent-jobs client 10 0 nil))
  ([client per-page]
   (get-recent-jobs client per-page 0 nil))
  ([client per-page page]
   (get-recent-jobs client per-page page nil))
  ([client per-page page problem]
    (let [sql         (str
                        "select j.idjob, j.submitted, j.resolved,j.solution, j.idcustomer, c.customername, j.status,
                           j.idproblem, j.problem, j.metadata
                            from job j
                            join customer c
                            using (idcustomer)
                            where idcustomer like ? "
                        (if (nil? problem)
                           "and (idproblem like ? or idproblem is null) and status='ready'"
                           "and idproblem like ? and status like '%' ")
                           "order by resolved desc
                            limit ?
                            offset ?")
          client      (or client "%")
          problem     (or problem "%")
          limit       (or per-page 0)
          offset      (* limit (or page 0)) ]
      (j/query db-conn [sql client problem limit offset] :row-fn keywordize-keys))))

(defn get-customer-jobs [{:keys [idcustomer per-page page] :as args :or {per-page 10 page 0}}]
  (let [idcustomer    (or idcustomer "%")
        sql           "select j.idjob, j.submitted, j.resolved,j.solution, j.idcustomer, c.customername, j.status,
                      j.idproblem, j.problem, j.metadata
                      from job j
                      join customer c
                      using (idcustomer)
                      where idcustomer like ? and status='ready'
                      order by resolved desc
                      limit ?
                      offset ?"]
    (j/query db-conn [sql idcustomer per-page (* per-page page)] :row-fn keywordize-keys)))

(defn get-problem-jobs [{:keys [idproblem per-page page] :as args :or {per-page 10 page 0}}]
  (let [sql           "select j.idjob, j.submitted, j.resolved, j.idcustomer, c.customername, j.status,
                      j.idproblem,  j.metadata
                      from job j
                      join customer c
                      using (idcustomer)
                      where status like '%'
                      and j.idproblem like ?
                      order by resolved desc
                      limit ?
                      offset ?"]
    (j/query db-conn [sql idproblem per-page (* per-page page)] :row-fn keywordize-keys)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; P R O B L E M ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def problem-state    #(cond
                          (< 0 (:solutions %))                 :solved
                          (and (:idmapping %) (:processed %))  :processed
                          (and (:processed %) (:mapped %))     :mapping
                          (:processed %)                       :unmapped
                          :else                                :processing))
(defn get-problem [id]
  (let [sql           "select p.*, count(j.idjob) solutions, m.column_map
                      from problem p
                      left join job j on p.idproblem=j.idproblem
                      left join mapping m on p.idmapping=m.idmapping
                      where p.idproblem=?
                      group by p.idproblem, m.idmapping"
        p             (first (j/query db-conn [sql id] :row-fn keywordize-keys))]
    (when p (assoc p :state (problem-state p)))))

(defn add-problem [obj]
  (let [idproblem       (genid)]
  (j/insert! db-conn :problem (assoc obj :idproblem idproblem))
  idproblem))

(defmulti update-problem :command)

(defmethod update-problem :add-mapping-time [args]
  (let [data          {:mapped (:operand args)}]
    (j/update! db-conn :problem data ["idproblem=?" (:idproblem args)])))

(defmethod update-problem :add-truck [args]
  (j/execute! db-conn ["update problem set trucks = array_append(trucks,?) where idproblem=?" (:operand args) (:idproblem args)]))

(defmethod update-problem :remove-truck [args]
  (j/execute! db-conn ["update problem set trucks = array_remove(trucks,?) where idproblem=?" (:operand args) (:idproblem args)]))

(defmethod update-problem :replace-trucks [args]
  (let [data          {:trucks (:operand args)}]
    (j/update! db-conn :problem data ["idproblem=?" (:idproblem args)])))

(defmethod update-problem :replace-depot [args]
  (let [data          {:depot (:operand args)}]
    (j/update! db-conn :problem data ["idproblem=?" (:idproblem args)])))

(defmethod update-problem :replace-orders [args]
  (let [data          {:orders (:operand args)}]
    (j/update! db-conn :problem data ["idproblem=?" (:idproblem args)])))

(comment
(defn update-problem [{:keys [idproblem] :as problem}]
  (j/update! db-conn :problem (dissoc problem :idproblem) ["idproblem=?" idproblem])))

(defn get-problem-count-by-depot [iduser]
  (let [sql           ["select depot, count(*) files from problem where iduser=? group by depot" iduser]]
    (j/query db-conn sql)))

(defn get-problem-count [{:keys [iduser idcustomer iddepot]}]
  (let [client        (or iddepot iduser idcustomer "%")
        sql-where     (str " where " (cond iddepot "depot like ?" iduser "iduser like ?" :else "idcustomer like ?"))
        sql           (str "select count(*) cnt from problem" sql-where)]
    (first (j/query db-conn [sql client]))))

(defn get-recent-problems [{:keys [iduser idcustomer iddepot per-page page] :as args :or {per-page 10 page 0}}]
  (let [state-fn        #(cond
                          (< 0 (:solutions %))                                   :solved
                          (and (:idmapping %) (:processed %))                    :processed
                          (and (:processed %) (:mapped %))                       :mapping
                          (:processed %)                                         :unmapped
                          :else                                                  :processing)
        sql-from        "select p.idproblem, p.label, p.submitted, p.processed, count(j1.idjob) solutions, count(j2.idjob) pending,
                         p.idmapping, p.mapped
                        from problem p
                        left join job j1 on p.idproblem=j1.idproblem and j1.status='ready'
                        left join job j2 on p.idproblem=j2.idproblem and j2.status='pending' "
        sql-where       (cond
                          iddepot       " where p.depot like ?"
                          iduser        " where p.iduser like ?"
                          idcustomer    " where p.idcustomer like ?")
        sql-rest        " group by p.idproblem
                        order by p.submitted desc
                        limit ?
                        offset ?"
        sql             (str sql-from sql-where sql-rest)
        where           (or iddepot iduser idcustomer)
        queryargs       (filterv identity [sql where per-page (* per-page page)])
        p               (try (j/query db-conn queryargs) (catch Exception e (log/error e)))]
    (mapv #(assoc % :state (problem-state %)) p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; C U S T O M E R ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn add-customer [obj]
  (let [idcustomer  (genid)
        customer    (assoc (select-keys obj [:customername :phone]) :idcustomer idcustomer :customer_code (:code obj))
        address     (assoc (select-keys obj [:street :suite :city :state :postalcode]) :idaddress (genid))]
    (j/with-db-transaction [t-con db-conn]
      (j/insert! t-con :address address)
      (j/insert! t-con :customer (assoc customer :idaddress (:idaddress address))))
    customer))

(defn get-customer [idcustomer]
  (first (j/query db-conn ["select * from vw_customer_address where idcustomer=?" idcustomer] :row-fn keywordize-keys)))

(defn update-customer [{:keys [customername idcustomer] :as obj}]
  (let [old           (first (j/query db-conn ["select * from address where idaddress=?" (:idaddress obj)]))
        idaddress     (get-address-id old obj) ]
    (j/update! db-conn :customer {:customername customername :idaddress idaddress} ["idcustomer=?" idcustomer])))

(defn update-source [{:keys [idcustomer customer]}]
  (let [data          {:customer_data customer}]
    (j/update! db-conn :customer data ["idcustomer=?" idcustomer])))

(defn update-capacity [args]
  (let [data          (select-keys args [:cap_config])]
    (j/update! db-conn :customer data ["idcustomer=?" (:idcustomer args)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; U S E R ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn check-email [email]
  (when-not (first (j/query db-conn ["select * from login where login=?" email])) :ok))

(defn add-user [obj]
  (let [user      (assoc obj :iduser (genid))]
    (j/insert! db-conn :login user)
    user))

(defn update-user [{:keys [iduser] :as  obj}]
  (j/update! db-conn :login (dissoc obj :iduser) ["iduser=?" iduser]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; COLLECTION FUNCTIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; A D D R E S S E S;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; D E P O T S ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-depots-by-user [iduser])
(defn get-depots-by-customer [idcustomer])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; T R U C K S ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-trucks-by-user [iduser])
(defn get-trucks-by-customer [idcustomer])
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; J O B S ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-jobs                [{:keys [limit offset] :as args}])
(defn get-jobs-by-problem     [{:keys [idproblem limit offset] :as args}])
(defn get-jobs-by-user        [{:keys [iduser offset limit] :as args}])
(defn get-jobs-by-customer    [{:keys [idcustomer offset limit] :as args}])
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; P R O B L E M S ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; U S E R S ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-user-by-email       [email]
  (let [q                     ["select * from login where login=?" email]]
    (first (j/query db-conn q))))

(defn get-user-by-id          [iduser]
  (let [q                     ["select * from login where iduser=?" iduser]]
    (first (j/query db-conn q))))

(defn get-user-by-lphash      [lphash]
  (let [q                     ["select * from login where lphash=?" lphash]]
    (first (j/query db-conn q))))

(def users-query              "select u.*, c.customername from login u join customer c using (idcustomer)")
(defn get-users               [{:keys [limit offset]}]
  (let [q                     (str users-query " order by created desc limit ? offset ?")
        res                   (j/query db-conn [q limit offset] :row-fn keywordize-keys)]
    (into [] res)))
(defn get-users-by-customer   [{:keys [idcustomer limit offset]}]
  (let [q                     (str users-query "where idcustomer=? order by created desc limit ? offset ?")
        res                   (j/query db-conn [q idcustomer limit offset])]
    (into [] res)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; C U S T O M E R S;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-customers           [{:keys [limit offset]}])
(defn get-customers-by-state  [{:keys [state limit offset] :as args}])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; S T A T E ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-user-state [user]
  (let [id      (:iduser user)
        u       (first
                  (j/query
                    db-conn
                    ["select * from login where iduser= ?" id]
                    :row-fn #(dissoc % :passwd)))
        d      (into
                  []
                  (j/query
                    db-conn
                    ["select * from depot d join address a using (idaddress) where iduser=? and (deleted <> true or deleted is null)
                     order by d.created desc" (:iduser u)]))
         tr     (into
                  []
                  (j/query
                    db-conn
                    ["select * from territory where iddepot in (select iddepot from depot where iduser=?)" (:iduser u)]))
         c      (first
                  (j/query
                    db-conn
                    ["select * from customer left join address using (idaddress) where idcustomer = ?" (:idcustomer u)]))
         t      (into
                  []
                  (j/query
                    db-conn
                    ["select t.*,d.label from truck t left join depot d using (iddepot) where t.iduser = ? and
                     (t.deleted <> true or t.deleted is null) order by created desc" (:iduser u)]))]
    (assoc
     {}
     :user u
     :customer c
     :territries tr
     :trucks t
     :depots d
     :problems (get-problem-count-by-depot (:iduser user)))))

(defn get-admin-state [user]
  (let [id      (:iduser user)
        u       (first
                  (j/query
                    db-conn
                    ["select * from login where iduser= ?" id]
                    :row-fn #(dissoc % :passwd :created)))
        us     (j/query
                  db-conn
                  ["select u.*,c.customername from login u join customer c using (idcustomer)"]
                  :row-fn #(dissoc % :passwd))
        cs     (j/query
                   db-conn
                   ["select c.idcustomer, c.customername, a.* from customer c left join address a using (idaddress)"])
         ]
    (assoc
     {}
     :user u
     :customers cs
     :users us)))
