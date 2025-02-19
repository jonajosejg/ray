(ns co.rowtr.mailer)


(defprotocol IMail
  (html!        [this])
  (text!        [this])
  (mail!        [this])
  (data!        [this data])
  (state?       [this])
  (check-state  [this]))
