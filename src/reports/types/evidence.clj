(ns reports.types.evidence
  "Check that Evidence Records tally with Events.
  Note that the date of Evidence may mean that it occurs the day before its Events are registerd in the Bus.
  Therefore for Query API check and Event BUs check we also look at the next day's storage.
   Scan all Evidence Records for the day:
    - find Event IDs missing from Query API
    - find Event IDs missing in Event Bus day-index
  We rely on another report for making sure the daily index matches the Event Storage in the Bus."
  (:require [event-data-common.storage.store :as store]
            [event-data-common.storage.store :refer [Store]]
            [event-data-common.storage.s3 :as s3]
            [reports.storage :as storage]
            [config.core :refer [env]]
            [clojure.tools.logging :as log]
            [clj-time.format :as clj-time-format]
            [clj-time.core :as clj-time]
            [clojure.data.json :as json]
            [clj-http.client :as client])
  (:gen-class))

(def query-api-endpoint
  "https://query-all.eventdata.crossref.org")

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(def evidence-ymd-prefix-format (clj-time-format/formatter "yyyyMMdd"))

(defn event-ids-from-evidence-records
  "Retrieve all Events contained in the Evidence Record"
  [date]
  (let [evidence-prefix (str "evidence/" (clj-time-format/unparse evidence-ymd-prefix-format date))
        evidence-record-keys (store/keys-matching-prefix @storage/evidence-storage evidence-prefix)
        counter (atom 0)
        total-evidence-records (count evidence-record-keys)]

    (set
      (apply concat
        (pmap
          (fn [evidence-record-key]
            (swap! counter inc)
            (when (and (zero? (rem @counter 1000))
                       (not (zero? total-evidence-records)))
              (log/info "Fetched Evidence Records" (float (/ @counter total-evidence-records))))

            (let [evidence-record (json/read-str (store/get-string @storage/evidence-storage evidence-record-key) :key-fn keyword)
                  events (mapcat (fn [page] (mapcat :events (:actions page))) (:pages evidence-record))
                  ; Drop leading "d/2017-04-23/" to get Event ID.
                  event-ids (map :id events)]

              event-ids))
          evidence-record-keys)))))

(defn event-ids-extant-in-daily-index
  "Is this event ID in the daily archive for these dates."
  [dates event-id]
  (first
    (keep
      (fn [date]
        (first
          (store/keys-matching-prefix @storage/event-bus-storage (str "d/" (clj-time-format/unparse ymd-format date) "/" event-id))))
      dates)))

(defn event-id-extant-in-query
  [event-id]
  "Spot-check that Event ID exists in the Query API."
  (= 200 (:status (client/get (str query-api-endpoint "/events/" event-id) {:throw-exceptions false}))))

(defn event-ids-from-date-index
  [date]
  (prn "Fetch events IDS from date index" date)
  (let [index-prefix (str "d/" (clj-time-format/unparse ymd-format date))
        event-keys (store/keys-matching-prefix @storage/event-bus-storage index-prefix)
        ; remove leading prefix.
        event-ids (map #(.substring % 13) event-keys)]
    event-ids))

(defn run
  [date daily-events]
  (let [; Event IDs from Query API for this day.
        from-query-api (set (map :id daily-events))
        ; Event IDs from Evidence Records produced on this day.
        from-evidence-records (event-ids-from-evidence-records date)
        ; Event DIs from the Event Bus' daily index.
        from-daily-index (set (event-ids-from-date-index date))
                
        ; Event IDs that are present in Evidence Records, but didn't make it into the Daily Index on that day.
        evidence-not-same-daily (clojure.set/difference from-evidence-records from-daily-index)

        ; Then spot-check each missing one for the next day's index for those that crossed midnight during processing.
        ; What remains is Event IDs that aren't found in either day.
        evidence-also-not-next-daily (remove #(event-ids-extant-in-daily-index [(clj-time/plus date (clj-time/days 1))] %) evidence-not-same-daily)
        evidence-not-daily-count evidence-also-not-next-daily

        ; Event IDs that are present in the Daily Index but didn't make it into the Query API.
        daily-not-evidence (clojure.set/difference from-daily-index from-query-api)

        ; Event IDs that are present in the Evidence Records but didn't make it into the Query API for that day.
        evidence-not-day-query (clojure.set/difference from-evidence-records from-query-api)
        
        ; As above, these may have crossed the date line in processing, so spot-check remainder one-by-one.
        evidence-not-query (remove event-id-extant-in-query evidence-not-day-query)]
  
  {:warnings (+ (count evidence-not-daily-count) (count daily-not-evidence) (count evidence-not-query))
   :human-data {
     :evidence-not-daily-count (count evidence-not-daily-count)
     :daily-not-evidence-count (count daily-not-evidence)
     :evidence-not-query-count (count evidence-not-query)

   :machine-data {
     :evidence-not-daily-count (count evidence-not-daily-count)
     :daily-not-evidence-count (count daily-not-evidence)
     :evidence-not-query-count (count evidence-not-query)

     :evidence-not-daily evidence-not-daily-count
     :daily-not-evidence daily-not-evidence
     :evidence-not-query evidence-not-query
     }}}))

(def manifest
  {:run run
   :human-name "Evidence"})
