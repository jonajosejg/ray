(ns co.rowtr.castra.core
  (:require
   [ring.middleware.resource              :refer [wrap-resource]]
   [ring.middleware.session.cookie        :refer [cookie-store]]
   [ring.middleware.file                  :refer [wrap-file]]
   [ring.middleware.content-type          :refer [wrap-content-type]]
   [ring.middleware.file-info             :refer [wrap-file-info]]
   [ring.middleware.multipart-params      :refer [wrap-multipart-params]]
   [ring.middleware.params                :refer [wrap-params]]
   [ring.middleware.reload                :refer [wrap-reload]]
   [ring.middleware.session               :refer [wrap-session]]
   [ring.middleware.session.cookie        :refer [cookie-store]]
   [ring.middleware.cors                  :refer [wrap-cors]]
   [ring.util.request                     :refer [body-string]]
   [adzerk.env                  :as env]
   [tailrecursion.cljson                  :refer [clj->cljson cljson->clj]]
   [co.rowtr.castra.api.user]
   [co.rowtr.castra.api.admin]
   [castra.middleware                     :refer [wrap-castra
                                                  wrap-castra-session
                                                  clj->json
                                                  json->clj]]))
;(reset! clj->json #(clj->cljson %2))
;(reset! json->clj #(cljson->clj %2))

(defn not-found-handler [req]
  {:status 200 :headers {"Content-Type" "text/plain"}})

(defn wrap-dump-request [handler]
  (fn [req]
      (println req)
      (handler req)))

(def app (->  not-found-handler
              (wrap-castra          'co.rowtr.castra.api.user 'co.rowtr.castra.api.admin)
;             (wrap-session         {:store (cookie-store {:key "tLqIU72r6vB5glOF"}) :cookie-attrs {:max-age 3600}})
              (wrap-castra-session  "tLqIU72r6vB5glOF")
              (wrap-cors            #".*")
              (wrap-reload)))
