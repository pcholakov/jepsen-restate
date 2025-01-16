(defproject restate.jepsen "0.1.0-SNAPSHOT"
  :description "Restate Jepsen tests"
  :main restate.jepsen
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen/jepsen "0.3.7"]
                 [clj-http "3.13.0"]
                 [cheshire "5.13.0"]]
  :plugins [[lein-ancient "1.0.0-RC3"]])
