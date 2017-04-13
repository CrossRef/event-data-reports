(ns reports.types.reddit
  "Simple stats for Reddit events
    - distinct subreddits
    - count per subreddit
    - subreddits not mentioned in the subreddit-artifact"
   (require [crossref.util.doi :as cr-doi]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.core :as clj-time]
            [event-data-common.artifact :as artifact]))

(defn run
  [date daily-events]
  (let [reddit-events (filter #(= "reddit" (:source_id %)) daily-events)
        subreddits (map #(->> % :subj_id (re-find #"(/r/.*?)/") second clojure.string/lower-case) reddit-events)
        distinct-subreddits (set subreddits)
        subreddit-counts (frequencies subreddits)
        artifact-subreddits (set (map clojure.string/lower-case
                                      (.split "\n" (artifact/fetch-latest-artifact-string "subreddit-list"))))
                                      
        ; subreddits in which events were found but aren't yet in the subreddit artifact.
        ; this artifact is used for a different source (reddit-links).
        missing (clojure.set/difference distinct-subreddits artifact-subreddits)]

  {:warnings 0
   :human-data {
     :subreddit-count (count distinct-subreddits)
     :subreddits distinct-subreddits
     :subreddit-counts subreddit-counts
     :missing-from-artifact missing}
   :machine-data {
     :subreddit-count (count distinct-subreddits)
     :subreddits distinct-subreddits
     :subreddit-counts subreddit-counts
     :missing-from-artifact missing}}))

(def manifest
  {:run run
   :human-name "Reddit"})
