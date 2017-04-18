(ns reports.types.domains
  "Simple stats for Domains of Events"
   (require [crossref.util.doi :as cr-doi]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.core :as clj-time]
            [event-data-common.artifact :as artifact])
   (import [java.net URL]))

(defn hostname [url-str]
  (when url-str
    (clojure.string/lower-case (.getHost (new URL url-str)))))

(defn run
  [date daily-events]
  (let [subj-domains (map #(-> % :subj_id hostname) daily-events)
        obj-domains (map #(-> % :obj_id hostname) daily-events)

        subj-url-domains (concat (keep #(-> % :subj :url hostname) daily-events)
                                 (keep #(-> % :subj :URL hostname) daily-events))
        
        obj-url-domains (concat (keep #(-> % :obj :url hostname) daily-events)
                                 (keep #(-> % :obj :URL hostname) daily-events))

        distinct-subj-domains (set subj-domains)
        distinct-obj-domains (set obj-domains)
        distinct-subj-url-domains (set subj-url-domains)
        distinct-obj-url-domains (set obj-url-domains)

        subj-domain-counts (frequencies subj-domains)
        obj-domain-counts (frequencies obj-domains)
        subj-url-domain-counts (frequencies subj-url-domains)
        obj-url-domain-counts (frequencies obj-url-domains)]

  {:warnings 0
   :human-data {
    ; The full lists are so long that only the minimum is useful in an email summary.
    :obj-domain-count (count distinct-obj-domains)
    :obj-url-domain-count (count distinct-obj-url-domains)
    :obj-url-domain-counts obj-url-domain-counts

    :subj-domain-count (count distinct-subj-domains)
    :subj-url-domain-count (count distinct-subj-url-domains)
    :subj-url-domain-counts subj-url-domain-counts}

   :machine-data {
     :obj-domain-count (count distinct-obj-domains)
     :obj-domain-counts obj-domain-counts
     :obj-domains distinct-obj-domains
     :obj-url-domain-count (count distinct-obj-url-domains)
     :obj-url-domain-counts obj-url-domain-counts
     :obj-url-domains distinct-obj-url-domains
     :subj-domain-count (count distinct-subj-domains)
     :subj-domain-counts subj-domain-counts
     :subj-domains distinct-subj-domains
     :subj-url-domain-count (count distinct-subj-url-domains)
     :subj-url-domain-counts subj-url-domain-counts
     :subj-url-domains distinct-subj-url-domains}}))

(def manifest
  {:run run
   :human-name "Domains"})
