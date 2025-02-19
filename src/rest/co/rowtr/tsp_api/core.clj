(ns co.rowtr.tsp-api.core
  (:require
    [liberator.dev                           :refer [wrap-trace]]
    [adzerk.env                  :as env]
    [ring.middleware.cors                    :refer [wrap-cors]]
    [ring.middleware.params                  :refer [wrap-params]]
    [ring.middleware.stacktrace              :refer [wrap-stacktrace]]
    [ring.middleware.reload                  :refer [wrap-reload]]
    [ring.middleware.basic-authentication    :refer [wrap-basic-authentication]]
    [co.rowtr.tsp-api.api                    :refer [app-routes authenticated?]]))

(def app
  (-> app-routes
      (wrap-basic-authentication authenticated?)
      (wrap-params)
      (wrap-reload)
      (wrap-cors            #".*")))
