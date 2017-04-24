(ns reports.storage
  (:require [event-data-common.storage.store :as store]
            [event-data-common.storage.store :refer [Store]]
            [event-data-common.storage.s3 :as s3]
            [config.core :refer [env]]
            [clojure.tools.logging :as l])
  (:gen-class))


(def report-storage
  (delay (s3/build (:s3-key env) (:s3-secret env) (:report-region-name env) (:report-bucket-name env))))

(def event-bus-storage
  (delay (s3/build (:bus-s3-key env) (:bus-s3-secret env) (:bus-region-name env) (:bus-bucket-name env))))

(def evidence-storage
  "S3 bucket where evidence is stored.
  This is currently in the same bucket as the reports so we can use the same creds."
  report-storage)


