(ns reports.mail
  (:require [clj-time.core :as time]
            [clj-time.coerce :as coerce]
            [clojure.tools.logging :as log]
            [config.core :refer [env]])
  (:import [java.util Properties]
           [javax.mail Session Message]
           [javax.mail.internet MimeMessage InternetAddress]))

(def port 25)

(defn send-mail [subject body from recipient]
  (let [props (System/getProperties)]
    (.put props "mail.transport.protocol" "smtp")
    (.put props "mail.smtp.port" port)
    (.put props "mail.smtp.auth" "true")
    (.put props "mail.smtp.starttls.enable" "true")
    (.put props "mail.smtp.starttls.required" "true")
    
    (let [session (Session/getDefaultInstance props)
          message (new MimeMessage session)
          transport (.getTransport session)]
      (.setFrom message (new InternetAddress from))
      (.setRecipient message javax.mail.Message$RecipientType/TO (new InternetAddress recipient))
      (.setSubject message subject)
      (.setContent message body "text/html")
      (.connect transport (:smtp-host env) (:smtp-username env) (:smtp-password env))
      (.sendMessage transport message (.getAllRecipients message)))))
