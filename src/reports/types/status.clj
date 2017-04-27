(ns reports.types.status
  "Take snapshot from the status service."
  (:require [clj-time.format :as clj-time-format]
            [clojure.data.json :as json]
            [clj-http.client :as client]
            [clojure.java.io :as io]))

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))


(def status-api "https://status.eventdata.crossref.org")

(defn fetch-status-snapshot
  "Fetch a snapshot of yesterday's events."
  [date]
  (let [date-str (clj-time-format/unparse ymd-format date)
        url (str status-api "/status/" date-str)
        response (client/get url {:as :stream :timeout 900000})
        body (json/read (io/reader (:body response)))]
    body))

(defn run
  [date daily-events _]
  (let [status-snapshot (fetch-status-snapshot date)]

  {:warnings 0
   :human-data {}
   :machine-data {
     :status status-snapshot}}))

(def manifest
  {:run run
   :human-name "Status"})
