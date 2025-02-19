(ns co.rowtr.castra.rules
  (:require
    [co.rowtr.common.data   :as d]
    [digest                       :refer [md5]]
    [castra.core                  :refer [ex *request* *session*]]))


(defn allow         []      (constantly true))
(defn deny          []      #(throw (ex "Access Denied!" {})))
(defn do-login!     [user]  (swap! *session* assoc :user (select-keys user [:idcustomer :iduser :role])))
(defn logged-in?    []      (do
                              (or (get @*session* :user)
                                  (throw  (ex "Please Log in." {:status 403})))))
(defn logout!       []      (reset! *session* nil))

(defn self? [iduser]
  (= iduser (-> @*session* :user :iduser)))

(defn customer? [idcustomer]
  (= idcustomer (-> @*session* :user :idcustomer)))

(defn owner? [{:keys [entity eid] :as args}]
  (let [iduser      (-> @*session* :user :iduser)
        ent         (d/get-entity-by-id args)]
    (when-not (= (:iduser ent) iduser) (throw (ex "You didn't build this!" {})))))

(defn admin? []
  (= (-> @*session* :user :role) "admin"))

(defn valid-role? [fns]
  (when-not (some true? fns) (throw (ex "Not Authorized" {}))))

; check-role accepts a set of roles and verifies set contains users's role
(defn check-role [roles]
  (let [urole       (-> @*session* :user :role)]
    (when (contains? roles urole) true)))

(defn login!        [user pass & [role]]
  (let [login-hash      (md5 (str user pass))
        rec             (d/validate-login user login-hash role)]
    (if
      rec
      (do-login! rec)
      (throw (ex "Authorization Failed" {})))))

(defn act-as-user [target user pass]
  (let [login-hash      (md5 (str user pass))
        admin?          (d/validate-login user login-hash :admin)]
    (if admin?
      (let [rec         (d/get-user-by-email target)]
        (if rec
          (do-login! rec)
          (throw (ex "Target user not found" {:target "not found"}))))
      (throw (ex "Not Authorized!" {})))))
