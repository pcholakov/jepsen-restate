(ns restate.jepsen.support
  (:require
   [clojure.string :as str]))

(defn restate-ingress-url
  "An HTTP url for connecting to the Restate ingress endpoint."
  [node port]
  (str "http://" node ":" port))
