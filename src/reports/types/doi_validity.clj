(ns reports.types.doi-validity
  "Check up on valid DOIs. 
   - DOIs that don't conform to the regex
   - DOIs that don't exist
   - DOIs that contain stray '?' and '#'."
  (require [crossref.util.doi :as cr-doi]
           [clj-http.client :as client]
           [clojure.tools.logging :as log]
           [clojure.core.cache :as cache]
           [clojure.core.memoize :as memo]
           [robert.bruce :refer [try-try-again]]))

(def good-doi-regex
  "Bear minumum to look like a DOI. Regression test for an earlier bug (and maybe future ones too).
   https://github.com/CrossRef/event-data-percolator/issues/31"
  #"^https?://(dx\\.)?doi.org/\d+\.\d+/.*$")

(def dois-checked (atom 0))

(defn doi-exists?
  [doi-url]
  (future
    (try-try-again
      {:sleep 30000}
      #(let [doi (cr-doi/non-url-doi doi-url)
            result (client/get (str "http://doi.org/api/handles/" doi) {:throw-exceptions false})]
        (when (zero? (mod (swap! dois-checked inc) 1000))
          (log/info "Checked" @dois-checked "DOIs"))
        (= (:status result) 200)))))

(def doi-exist-cached
  (memo/lu doi-exists? :lu/threshold 100000))

(defn run
  [date daily-events _]
  (let [; DOI-like strings
        doi-like (filter #(re-find #"https?://(dx\\.)?doi.org" %)
                   (mapcat (fn [event] [(:subj_id event) (:obj_id event)]) daily-events))

        not-matching (remove (partial re-matches good-doi-regex) doi-like)
        count-not-matching (count not-matching)

        containing-qmark (filter #(.contains % "?") doi-like)
        count-containing-qmark (count containing-qmark)

        containing-hash (filter #(.contains % "#") doi-like)
        count-containing-hash (count containing-hash)

        ; Split into chunks, fetch each chunk in parallel.
        doi-like-in-chunks (partition-all 64 doi-like)
        chunks-c (atom 0)
        doi-existence-in-chunks (map (fn [doi-like-chunk]
                                       (doall (map (fn [doi] [doi (doi-exist-cached doi)]) doi-like-chunk))) doi-like-in-chunks)

        non-existant-in-chunks (map (fn [chunk]
                                      (map first (remove #(-> % second deref) chunk))) doi-existence-in-chunks)

        non-existant (apply concat non-existant-in-chunks)

        non-existant-count (count non-existant)]

    {:warnings (+ non-existant-count count-containing-qmark count-not-matching count-containing-hash)
     :human-data {:not-matching-regex not-matching
                  :containing-question-mark containing-qmark
                  :non-existant non-existant
                  :containing-hash containing-hash}
     :machine-data {:not-matching-regex not-matching
                  :containing-question-mark containing-qmark
                  :non-existant non-existant
                  :containing-hash containing-hash}}))

(def manifest
  {:run run
   :human-name "DOI Validity"})
