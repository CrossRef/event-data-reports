(ns reports.report-types
  "Report type registry. Every report type's run function should return a hash-map with the following keys:
   - :machine-data - anything
   - :human-data - anything
   - :warnings - number"
  (require [reports.types.events :as events]
           [reports.types.twitter-ids :as twitter-ids]
           [reports.types.doi-validity :as doi-validity]
           [reports.types.status :as status]
           [reports.types.reddit :as reddit]
           [reports.types.stackexchange :as stackexchange]
           [reports.types.domains :as domains]))

(def all-manifests
  {:events events/manifest
   :twitter-ids twitter-ids/manifest
   :doi-validity doi-validity/manifest
   :status status/manifest
   :reddit reddit/manifest
   :stackexchange stackexchange/manifest
   :domains domains/manifest})

