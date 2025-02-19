(ns co.rowtr.tsp-api.outfile
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream))
  (:require
    [co.rowtr.common.config               :as cfg       :refer [s3-creds]]
    [dk.ative.docjure.spreadsheet         :as sht]
    [clj-time.format                      :as f]
    [clj-time.core                        :as t]
    [clj-time.coerce                      :as c]
    [clojure.tools.logging                :as log]
    [co.rowtr.common.util                 :as u         :refer [seconds->hhmmss meters->miles custom-fmt safe-add]]
    [amazonica.aws.s3                     :as s3]))


(defn compute-totals [stops]
  (reduce
    (fn [cum m]
      (assoc
        cum
        :load                 (safe-add (:load cum) (:load m))
        :load_1               (safe-add (:load_1 cum) (:load_1 m))
        :driving-time         (safe-add (:driving-time cum) (:driving-time m))
        :driving-distance     (safe-add (:driving-distance cum) (:driving-distance m))
        :waiting-time         (safe-add (:waiting-time cum) (:waiting-time m))))
   {}
   stops))


(defn extract-columns [ks route]
  (mapv (fn [s] (mapv (fn [k] (or (k (:meta s)) (k s))) ks)) (:stops route)))

(defn route->sheet [{:keys [sheet file_columns route] :as args}]
  (let [totes     (compute-totals (:stops route))
        tz        (t/time-zone-for-id (:tz_id (:depot route)))
        stops     (mapv #(assoc % :eta  (u/fmt-local u/custom-fmt (:eta %) tz)
                                  :eta-in-traffic (u/fmt-local u/custom-fmt (:eta-in-traffic %) tz)
                                  :driving-time (seconds->hhmmss (:driving-time %))
                                  :driving-distance (meters->miles (:driving-distance %))
                                  :waiting-time (seconds->hhmmss (:waiting-time %)))
                        (:stops route))
        eta?      (some :eta stops)
        cols      (concat file_columns ["driving-time" "driving-distance" "waiting-time"] (when eta? ["eta" "eta-in-traffic"]))
        route     (assoc route :stops stops)
        ks        (mapv keyword cols)
        rows      (extract-columns ks route)
        rows      (conj (into [] (butlast rows)) (assoc-in (last rows) [0] "Return to Depot"))]
    (sht/add-row! sheet ["Truck" (-> route :truck :label)])
    (sht/add-row! sheet nil)
    (sht/add-row! sheet [(or (first (:cap_config args)) "Load") (:load totes)])
    (when (second (:cap_config args)) (sht/add-row! sheet [(second (:cap_config args)) (:load_1 totes)]))
    (when (:route_start route) (sht/add-row! sheet ["Route Start" (u/fmt-local u/custom-fmt (:route_start route) tz)]))
    (sht/add-row! sheet ["Route Duration" (seconds->hhmmss (:driving-time totes))])
    (sht/add-row! sheet ["Route Distance" (meters->miles (:driving-distance totes))])
    (sht/add-row! sheet ["Total Waiting Time" (seconds->hhmmss (:waiting-time totes))])
    (sht/add-row! sheet nil)
    (sht/add-row! sheet cols)
    (sht/add-rows! sheet rows)
    (mapv #(.autoSizeColumn sheet %) (range (count cols)))
    totes))

(defn make-workbook [{:keys [solution file_columns cap_config] :as args}]
  (let [sheets    (mapv #(-> % :truck :label) (:routes solution))
        wb        (sht/create-xls-workbook "Summary" nil)
        d         (mapv #(sht/add-sheet! wb %)  sheets)
        totes     (mapv
                    #(let [t  (-> % :truck :label)
                           s  (sht/select-sheet t wb)]
                       (route->sheet (merge args {:sheet s :route %})))
                    (:routes solution))
        sheet     (sht/select-sheet "Summary" wb)]
    (sht/add-row! sheet (apply vector "" sheets))
    (sht/add-row! sheet (apply vector "Route Duration" (map #(seconds->hhmmss (:driving-time %)) totes)))
    (sht/add-row! sheet (apply vector "Route Distance" (map #(meters->miles (:driving-distance %)) totes)))
    (sht/add-row! sheet (apply vector "Waiting Time"  (map #(seconds->hhmmss (:waiting-time %)) totes)))
    (sht/add-row! sheet nil)
    (sht/add-row! sheet ["Total Duraton" (seconds->hhmmss (reduce + (map :driving-time totes)))])
    (sht/add-row! sheet ["Total Distance" (meters->miles (reduce + (map :driving-distance totes)))])
    (sht/add-row! sheet ["Total Waiting" (seconds->hhmmss (reduce + (map :waiting-time totes)))])
    (mapv #(.autoSizeColumn sheet %) (-> solution :routes count inc range))
    wb))

(defn workbook->stream [wb]
  (let [os          (java.io.ByteArrayOutputStream.)
        d           (sht/save-workbook-into-stream! os wb)]
    os))

(defn stream->s3file [job stream]
  (let [bs          (.toByteArray stream)
        k           (str "solutions/" (:idcustomer job) "/" (:idproblem job) "/" (:idjob job) ".xls")]
    (s3/put-object cfg/s3-creds
                   :bucket-name "rowtr-solution"
                   :key k
                   :input-stream (java.io.ByteArrayInputStream. bs)
                   :metadata {:content-length (count bs)})))
