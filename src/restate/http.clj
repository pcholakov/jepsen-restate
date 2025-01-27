(ns restate.http
  (:require [clj-http.conn-mgr :as conn-mgr]))

(defn connection-manager [& opts]
  (conn-mgr/make-reusable-conn-manager
   (merge opts {:timeout 2
                :default-per-route 20
                :threads 100})))

