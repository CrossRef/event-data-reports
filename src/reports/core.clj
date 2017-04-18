(ns reports.core
  (:require [reports.mail :as mail]
            [reports.report-types :as report-types]
            [event-data-common.storage.store :as store]
            [event-data-common.storage.store :refer [Store]]
            [event-data-common.storage.s3 :as s3]
            [config.core :refer [env]]
            [clojure.tools.logging :as l]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.periodic :as clj-time-periodic]
            [json-html.core :as json-html]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clj-http.client :as client]
            [clojurewerkz.quartzite.triggers :as qt]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.schedule.daily-interval :as daily]
            [clojurewerkz.quartzite.schedule.calendar-interval :as cal]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.schedule.cron :as qc]
            [robert.bruce :refer [try-try-again]])
  (:gen-class))


(def report-storage
  (delay (s3/build (:s3-key env) (:s3-secret env) (:report-region-name env) (:report-bucket-name env))))

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(def query-api "https://query-all.eventdata.crossref.org")

(def report-prefix "r/")

(defn path-for-report-type-id
  [date report-type-id]
  (str report-prefix (clj-time-format/unparse ymd-format date) "/" (name report-type-id) ".json"))

(defn fetch-query-api-events
  "Fetch a lazy seq of events as reported as collected on this day."
  ([date] (fetch-query-api-events date ""))
  ([date cursor]
    (let [date-str (clj-time-format/unparse ymd-format date)
          url (str query-api "/events?filter=from-collected-date:" date-str ",until-collected-date:" date-str "&cursor=" cursor "&rows=5000")
          response (try-try-again {:sleep 30000 :tries 10} #(client/get url {:as :stream :timeout 900000}))
          body (json/read (io/reader (:body response)) :key-fn keyword)
          events (-> body :message :events)
          next-cursor (-> body :message :next-cursor)]
      (if next-cursor
        (lazy-cat events (fetch-query-api-events date next-cursor))
        events))))

(defn generate!
  "Generate all reports for this day and store them."
  [date missing-type-ids]
  (log/info "Generating" date "types" missing-type-ids)
  (let [events (fetch-query-api-events date)]
    (doseq [type-id missing-type-ids]
      (log/info "Generate report" type-id)
      (let [manifest (report-types/all-manifests type-id)
            result ((:run manifest) date events)
            decorated (assoc result :date (str date)
                                    :generated (str (clj-time/now))
                                    :type type-id)
            report-path (path-for-report-type-id date type-id)]
        (store/set-string @report-storage report-path (json/write-str decorated))))))

(defn report-exists?
  [date report-type-id]
  (let [report-path (path-for-report-type-id date report-type-id)]
    (= (store/keys-matching-prefix @report-storage report-path) [report-path])))

(defn missing-reports-for-date
  "Return report-type-ids of reports that do not exist for this date."
  [date]
  (let [report-type-ids (keys report-types/all-manifests)]
    (remove (partial report-exists? date) report-type-ids)))

(defn generate-if-missing!
  "Generate missing reports for this date, if there are any."
  [date]
  (let [missing-type-ids (missing-reports-for-date date)]
    (if (empty? missing-type-ids)
      (do
        (log/info "Skipping" date)
        nil)
      (generate! date missing-type-ids))))

(defn retrieve-reports
  [date]
  (let [prefix (str report-prefix (clj-time-format/unparse ymd-format date) "/")
        paths (store/keys-matching-prefix @report-storage prefix)]
    (map #(-> (store/get-string @report-storage %) (json/read-str :key-fn str)) paths)))

(defn render-reports
  [report-seq]
  (let [sections (map (fn [report]
                        (let [heading (get report "human-name")
                              content (json-html/edn->html (get report "human-data"))]
                          (str "<h2>" heading "</h2>" content))) report-seq)
        result (apply str sections)]
    result))

(defn send-emails!
  "Send emails for reports on the given day."
  [date]
  (let [reports (retrieve-reports date)
        warnings (reduce + (map #(get % "warnings") reports))
        html (render-reports reports)
        emails (.split (:emails env "") ",")
        warning-emails (.split (:warning-emails env "") ",")
        subject (if (> warnings 0)
                      (str "CED Report " (clj-time-format/unparse ymd-format date) " (" warnings " warnings!)")
                      (str "CED Report " (clj-time-format/unparse ymd-format date)))]
    (when (> warnings 0)
      (doseq [recipient warning-emails]
        (mail/send-mail subject html (:email-from env) recipient)))

      (doseq [recipient emails]
        (mail/send-mail subject html (:email-from env) recipient)))
  (log/info "Finished sending all emails"))


(defn run-daily!
  []
  (let [max-days (Integer/parseInt (:max-days env "10"))
        now (clj-time/now)
        start-date (clj-time-format/parse ymd-format (:epoch env))
        end-date (clj-time/minus now (clj-time/days 1))
        date-range (take-while #(clj-time/before? % end-date) (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))
        dates (take-last max-days date-range)]
    (log/info "Checking / generating historical reports...")
    (doseq [date dates]
      (log/info "Looking at" (clj-time-format/unparse ymd-format date))
      (generate-if-missing! date))
    (log/info "Generated all reports")
    (log/info "Mailing report for" (clj-time-format/unparse ymd-format end-date))))


(defjob daily-schedule-job
  [ctx]
  (log/info "Doing yesterday's reports...")
  (run-daily!)
  (send-emails! (clj-time/minus (clj-time/now) (clj-time/days 1)))
  (log/info "Done yesterday's reports."))

(defn run-archive-schedule!
  "Start schedule to generate daily reports. Block."
  []
  (log/info "Start scheduler")
  (let [s (-> (qs/initialize) qs/start)
        job (qj/build
              (qj/of-type daily-schedule-job)
              (qj/with-identity (qj/key "jobs.noop.1")))
        trigger (qt/build
                  (qt/with-identity (qt/key "triggers.1"))
                  (qt/start-now)
                  (qt/with-schedule (qc/cron-schedule "0 0 3 * * ?")))]
  (qs/schedule s job trigger)))

(defn -main
  "Fill in historical reports, then send email about most recent one."
  [& args]
  (condp = (first args)
    "daily" (do
              (run-daily!)
              (send-emails! (clj-time/minus (clj-time/now) (clj-time/days 1))))
    "schedule" (run-archive-schedule!)
    (log/error "Unrecognised command")))
