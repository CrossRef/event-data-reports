(ns reports.types.stackexchange
  "Simple stats for StackExchange events
    - distinct sites
    - count per site
    - sites not mentioned in the stackexchange-sites artifact"
   (:require [crossref.util.doi :as cr-doi]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.core :as clj-time]
            [event-data-common.artifact :as artifact]
            [clojure.data.json :as json])
   (:import [java.net URL]))

(defn hostname [url-str]
  (clojure.string/lower-case (.getHost (new URL url-str))))

(defn run
  [date daily-events]
  (let [se-events (filter #(= "stackexchange" (:source_id %)) daily-events)
        sites (map #(-> % :subj_id hostname) se-events)
        distinct-sites (set sites)
        site-counts (frequencies sites)

        artifact-sites (set (map #(-> % :site_url hostname) (json/read-str (artifact/fetch-latest-artifact-string "stackexchange-sites") :key-fn keyword)))
                                      
        ; sites in which events were found but aren't yet in the artifact.
        missing (clojure.set/difference distinct-sites artifact-sites)]

  {:warnings 0
   :human-data {
     :site-count (count distinct-sites)
     :sites distinct-sites
     :site-counts site-counts
     :missing-from-artifact missing}
   :machine-data {
     :site-count (count distinct-sites)
     :sites distinct-sites
     :site-counts site-counts
     :missing-from-artifact missing}}))

(def manifest
  {:run run
   :human-name "StackExchange"})
