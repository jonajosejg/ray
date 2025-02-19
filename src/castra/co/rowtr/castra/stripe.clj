(ns co.rowtr.castra.stripe
  (:require
    [clj-stripe.util              :as util]
    [clj-stripe.common            :as common]
    [clj-stripe.charges           :as charges]
    [clj-stripe.cards             :as cards]
    [clj-stripe.customers         :as customers]
    [clojure.pprint                                     :refer [pprint]]
    [castra.core                  :as castra            :refer [ex]]))


(defn new-customer [{:keys [secret-key source description email card] :as args}]
  (let [body          (customers/create-customer
                            (when email (customers/email email))
                            (when description (common/description description)))
        source        (when source {"source" source})
        card          (when card {"card" card})
        body          (merge body source card)
        resp          #(common/with-token secret-key
                        (common/execute body))]
    (let [resp  (resp)]
      (if
        (not (:id resp))
        (throw (ex (-> resp :error :message) resp))
        resp))))

(defn update-customer-source [{:keys [secret-key source customer card] :as args}]
  (let [body        (customers/update-customer customer)
        source      (when source {"source" source})
        card        (when card {"source" card})
        body        (merge body source card)
        resp        #(common/with-token secret-key
                       (common/execute body))]
    (let [resp  (resp)]
      (if
        (not (:id resp))
        (do
          (throw (ex (-> resp :error :message) resp)))
        resp))))

(defn retrieve-customer [{:keys [secret-key customer]}]
  (let [body        (customers/get-customer customer)
        resp        (common/with-token secret-key
                      (common/execute body))]
    (if (not (:id resp))
      (throw (ex (-> resp :error :message) resp))
      resp)))
