(ns reports.types.twitter-ids
  "Extract all Tweet and Twitter User IDs for later compliance checking.")

(defn tweet-pid-to-id
  [pid]
  (first (re-find #"(\d+)$" pid)))

(defn run
  [date daily-events]
  (let [tweet-events (filter #(= (:source_id %) "twitter") daily-events)
        tweet-id-event-id (into {} (map (fn [event] [(-> event :subj_id tweet-pid-to-id) (:id event)]) tweet-events))]
  {:warnings 0
    ; This is machine only.
   :human-data {}
   :machine-data {
     :tweet-id-event-id tweet-id-event-id}}))

(def manifest
  {:run run
   :human-name "Twitter IDs"})
