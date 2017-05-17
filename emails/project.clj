(defproject emails "0.1.0-SNAPSHOT"
  :description "sending emails in a batch"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main emails.core
  :aot [emails.core]
  :dependencies [[org.clojure/clojure "1.8.0"]]
  :target-path "target/%s"
  )
