(defproject reports "0.1.8"
  :description "Event Data Reports"
  :url "http://eventdata.crossref.org/"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]
                 [org.clojure/core.memoize "0.5.8"]
                 [event-data-common "0.1.20"]
                 [org.clojure/data.json "0.2.6"]
                 [crossref-util "0.1.10"]
                 [clj-http "3.4.1"]
                 [overtone/at-at "1.2.0"]
                 [robert/bruce "0.8.0"]
                 [yogthos/config "0.8"]
                 [clj-time "0.12.2"]
                 [com.amazonaws/aws-java-sdk "1.11.61"]
                 ; Required for AWS, but not fetched.
                 [org.apache.httpcomponents/httpclient "4.5.2"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.logging.log4j/log4j-core "2.6.2"]
                 [org.slf4j/slf4j-simple "1.7.21"]
                 [com.sun.mail/javax.mail "1.5.4"]
                 [json-html "0.4.0"]
                 [clojurewerkz/quartzite "2.0.0"]]
  :main ^:skip-aot reports.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
