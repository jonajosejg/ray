(ns co.rowtr.castra.email
  (:require
    [co.rowtr.common.data       :as d]
    [co.rowtr.mailer            :as mail]
    [co.rowtr.mailer.sesmailer  :as ses]
    [co.rowtr.storage           :as st]
    [co.rowtr.storage.s3storage :as s3s]))

(def from         "admin@rowtr.co")
(def base-url     "https://app.rowtr.co")
(def tpl-bucket   "rowtr-template")

(defn send-verification-email [{:keys [recipient vrhash]}]
  (let [data      {:vr-link (str base-url "#/verify/" vrhash)}
        mm        (ses/mail-machine
                    :from               from
                    :to                 recipient
                    :subject            "Rowtr Email Verification"
                    :mail-type          "verify"
                    :data               data
                    :logger             d/log-trx-email
                    :storage-provider   (s3s/make-s3-storage-provider :bucket tpl-bucket))]
    (mail/mail! mm)))

(defn send-password-reset-email [{:keys [recipient lphash]}]
  (let [data      {:lp-link (str base-url "#/reset/" lphash)}
        mm        (ses/mail-machine
                    :from               from
                    :to                 recipient
                    :subject            "Rowtr Password Reset"
                    :mail-type          "password-reset"
                    :data               data
                    :logger             d/log-trx-email
                    :storage-provider   (s3s/make-s3-storage-provider :bucket tpl-bucket))]
    (mail/mail! mm)))
