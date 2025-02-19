(ns co.rowtr.storage)

(defprotocol
  IStorage
  (put-form                 [this key form-data])
  (fetch-form               [this key])
  (fetch-email-templates    [this mail-type]))
