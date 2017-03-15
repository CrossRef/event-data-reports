(ns reports.types.events
  "Simple stats from Events
   - count all Events
   - count by DOI prefix
   - count of all DOIs
   - count of distinct DOIs
   - count per source_id
   - lag between occurrred and collected (just for interest)"
   (require [crossref.util.doi :as cr-doi]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.core :as clj-time]))

(defn lag
  "Calculate lag in seconds between two events. Can be negative."
  [start-date-str end-date-str]
  (let [start-date (clj-time-coerce/from-string start-date-str)
        end-date (clj-time-coerce/from-string end-date-str)]
    (if (clj-time/before? start-date end-date)
      (clj-time/in-seconds (clj-time/interval start-date end-date))
      (* (clj-time/in-seconds (clj-time/interval end-date start-date)) -1))))

(defn run
  [date daily-events]
  (let [event-count (count daily-events)
        ; DOIs found in subject or object position
        dois (filter #(cr-doi/well-formed %)
                  (mapcat (fn [event] [(:subj_id event) (:obj_id event)]) daily-events))

        ; Prefixes of DOIs found in subject or object position.
        prefixes (map #(cr-doi/get-prefix %) dois)

        prefix-count (count (distinct prefixes))
        prefix-frequencies (frequencies prefixes)

        distinct-doi-count (count (distinct dois))

        sources (map :source_id daily-events)

        distinct-source-count (count (distinct sources))

        source-frequencies (frequencies sources)

        ; Lag between occurred and collected in seconds.
        lags (map #(lag (:occurred_at %) (:timestamp %)) daily-events)
        average-lag (when (not-empty lags) (/ (reduce + lags) (count lags)))]

  {:warnings 0
   :human-data {
     :event-count event-count
     :prefix-count prefix-count
     :prefix-frequencies prefix-frequencies
     :distinct-doi-count distinct-doi-count
     :distinct-source-count distinct-source-count
     :source-frequencies source-frequencies
     :average-lag average-lag}
   :machine-data {
     :event-count event-count
     :prefix-count prefix-count
     :prefix-frequencies prefix-frequencies
     :distinct-doi-count distinct-doi-count
     :distinct-source-count distinct-source-count
     :source-frequencies source-frequencies
     :average-lag average-lag}}))

(def manifest
  {:run run
   :human-name "Events"})
