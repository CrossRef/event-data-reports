(ns reports.types.twitter-compliance
  "Check Tweet IDs against compliance data. Find tweet ids that have been included in Events"
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

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(defn twitter-compliance-prefix
  [date]
  (str "twitter/tweet-deletions/" (clj-time-format/unparse ymd-format date)))

(defn tweet-ids-for-key
  "Retrieve set of tweet IDs for the given S3 key."
  [k]
  (log/info "Retrieve tweet IDs for" k)
  (->>
    (json/read-str (store/get-string @storage/report-storage k) :key-fn keyword)
    :machine-data
    :tweet-id-event-id
    keys
    (map name)))

(defn all-tweet-ids-in-events-ever!
  "Scan all of the twitter-id reports and gather all of the tweets we have ever found in an Event.
   This requires scanning every historical day every time this report is generated, so this will take O(n).
   However this is only done once a day, and the number will be in the hundreds. Can refactor in a few years' time."
  []
  (let [all-keys (store/keys-matching-prefix @storage/report-storage "r/")
        twitter-keys (filter #(.endsWith % "/twitter-ids.json") all-keys)
        twitter-ids (mapcat tweet-ids-for-key twitter-keys)
        unique-twitter-ids (set twitter-ids)]
    (log/info "Found" (count twitter-ids) ", " (count unique-twitter-ids) "unique, from" (count twitter-keys) "daily reports.")
    unique-twitter-ids))

(defn matching-tweet-ids-for-day
  "Return a sequence of tweet ids that were deleted on the given day that were mentioned in any Events."
  [date tweet-ids-in-events]
  (let [deleted-chunk-keys (store/keys-matching-prefix @storage/report-storage (str "twitter/tweet-deletions/" (clj-time-format/unparse ymd-format date)))
        num-chunks (count deleted-chunk-keys)]
    ; Very deliberately making GC easy. Chunks are big.
    (loop [matching #{}
           [chunk-key & chunk-keys] deleted-chunk-keys
           total-compared 0
           chunks-compared 0]
      (let [chunk-content (set (map str (json/read-str (store/get-string @storage/report-storage chunk-key))))
            intersecting (clojure.set/intersection chunk-content tweet-ids-in-events)]
        (log/info "Compared" total-compared "IDs in" chunks-compared "chunks / " num-chunks "chunks. In this chunk" (count intersecting) "IDs intersect")
        (when-not (empty? intersecting)
          (log/info "Deleted tweets:" intersecting))
        (if (empty? chunk-keys)
          (clojure.set/union intersecting matching)
          (recur (clojure.set/union intersecting matching)
                 chunk-keys
                 (+ total-compared (count chunk-content))
                 (inc chunks-compared)))))))

(defn run
  [date daily-events scratch]
  ; Only fetch tweets-in-events once per run.
  (let [tweet-ids-in-events (:tweet-ids-in-events scratch (all-tweet-ids-in-events-ever!))
        matching-ids (matching-tweet-ids-for-day date tweet-ids-in-events)]
    
    ; Cache for next time.
    (assoc! scratch :tweet-ids-in-events tweet-ids-in-events)
    {:warnings (count matching-ids)
     :human-data {
       :deleted-tweet-ids matching-ids}
     :machine-data {
       :deleted-tweet-ids matching-ids}}))

(def manifest
  {:run run
   :human-name "Deleted Tweet IDs in Events"})
