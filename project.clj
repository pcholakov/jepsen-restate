(defproject restate.jepsen "0.1.0-SNAPSHOT"
  :description "Restate Jepsen tests"
  :main restate.jepsen
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen/jepsen "0.3.5"]

                 ;; [com.jcraft/jsch "0.1.55"]
                 [net.java.dev.jna/jna-platform "5.14.0"] ;; Missing libjna on arm64 with default

                 [clj-http "3.13.0"]
                 [cheshire "5.13.0"]])
