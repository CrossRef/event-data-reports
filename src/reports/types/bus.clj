(ns reports.types.bus
  "Check that the Event Bus storage is consistent.
   - Events that are present in Event Bus daily index but not in storage."
  (:require [event-data-common.storage.store :as store]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [event-data-common.storage.store :refer [Store]]
            [event-data-common.storage.s3 :as s3]
            [reports.storage :as storage]
            [clojure.tools.logging :as log])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client])
  (:gen-class))

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(defn event-ids-from-date-index
  [date]
  (prn "Fetch events IDS from date index" date)
  (let [index-prefix (str "d/" (clj-time-format/unparse ymd-format date))
        event-keys (store/keys-matching-prefix @storage/event-bus-storage index-prefix)
        ; remove leading prefix.
        event-ids (map #(.substring % 13) event-keys)]
    event-ids))

(defn find-events-missing-from-storage
  [event-ids]
  (let [^AmazonS3Client client (:client @storage/event-bus-storage)
        ^String bucket-name (:s3-bucket-name @storage/event-bus-storage)
        counter (atom 0)
        total (count event-ids)
        total-did-not-exist (atom 0)
        existence (pmap (fn [event-id]
                            (let [counter-val (swap! counter inc)
                                  exists (.doesObjectExist client bucket-name (str "e/" event-id))]
                              (when-not exists
                                (swap! total-did-not-exist inc))
                              (when (zero? (rem counter-val 1000))
                                (log/info "Checked" counter-val "/" total "found" @total-did-not-exist "didn't exist"))
                              
                            [event-id exists])) event-ids)
        missing-ids (map first (remove second existence))]
    missing-ids))

(defn run
  [date _]
  (let [; Event IDs from the Event Bus' daily index.
        event-ids (set (event-ids-from-date-index date))
        missing-from-bus (find-events-missing-from-storage event-ids)]
  {:warnings (count missing-from-bus)
   :human-data {
     :missing-from-bus-count (count missing-from-bus)}
   :machine-data {
    :missing-from-bus missing-from-bus
    :missing-from-bus-count (count missing-from-bus)}}))

(def manifest
  {:run run
   :human-name "Event Bus"})
