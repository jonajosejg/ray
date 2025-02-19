(ns co.rowtr.tsp-api.core
  (:require
    [ring.middleware.cors                    :refer [wrap-cors]]))

(defn not-found-handler [req]
  {:status 200 :headers {"Content-Type" "text/plain"}})

(def app
  (-> not-found-handler
      (wrap-cors            #".*")))
