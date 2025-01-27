(ns restate.jepsen.metadata-store-register
  "A CAS register client implemented on top of the Restate Metadata Store HTTP API.
  Note: restate-server must be compiled with the metadata-api feature."
  (:require
   [jepsen
    [checker :as checker]
    [client :as client]
    [generator :as gen]
    [independent :as independent]]
   [knossos.model :as model]
   [clj-http.client :as http]
   [cheshire.core :as json]
   [slingshot.slingshot :refer [try+]]
   [restate [http :as hu]]
   [restate.jepsen.common :refer [parse-long-nil]]
   [restate.jepsen.register-ops :refer [r w cas]]))

(defrecord
 RegisterClient [conn-mgr] client/Client

 (setup! [_this _test])

 (open! [this test node] (assoc this
                                :node (str "n" (inc (.indexOf (:nodes test) node)))
                                :admin-api (str "http://" node ":9070")
                                :defaults {:connection-manager conn-mgr
                                           :connection-timeout 500
                                           :socket-timeout 1000}
                                :random (new java.util.Random)))

 (invoke! [this _test op]
   (let [[k v] (:value op)]
     (try+
      (case (:f op)
        :read (try+ (let [value (->> (http/get (str (:admin-api this) "/metadata/" k)
                                               (:defaults this))
                                     (:body)
                                     (parse-long-nil))]
                      (assoc op :type :ok :value (independent/tuple k value) :node (:node this)))
                    (catch [:status 404] {}
                      (assoc op :type :ok :value (independent/tuple k nil) :node (:node this))))
        :write (do
                 (http/put (str (:admin-api this) "/metadata/" k)
                           (merge (:defaults this)
                                  {:body (json/generate-string v)
                                   :headers {:If-Match "*"
                                             :ETag (bit-shift-left (abs (.nextInt (:random this))) 1)}
                                   :content-type :json}))
                 (assoc op :type :ok :node (:node this)))

        :cas (let [[expected-value new-value] v
                   [stored-value stored-version]
                   (try+
                    (let [res (http/get (str (:admin-api this) "/metadata/" k)
                                        (:defaults this))]
                      [(parse-long-nil (:body res)) (parse-long-nil (->> res (:headers) (:ETag)))])
                    ;; this key is unset
                    (catch [:status 404] {} [nil nil]))]

               (if (= stored-value expected-value)
                 (do (http/put (str (:admin-api this) "/metadata/" k)
                               (merge (:defaults this)
                                      {:body (json/generate-string new-value)
                                       :headers {:If-Match stored-version
                                                 :ETag (inc stored-version)}
                                       :content-type :json}))
                     (assoc op :type :ok :node (:node this)))
                 (assoc op :type :fail :error :precondition-failed :node (:node this)))))

      (catch [:status 412] {} (assoc op :type :fail :error :precondition-failed :node (:node this)))

      ;; the MDS API returns 404 for unset keys
      (catch [:status 404] {} (assoc op :type :ok :value nil :node (:node this)))

      (catch java.net.ConnectException {} (assoc op :type :info :error :connect :node (:node this)))

      ;; NB: :type :info events indicate that the effect on the system is uncertain
      (catch [:status 500] {} (assoc op :type :info :error :server-internal :node (:node this)))
      (catch java.net.SocketTimeoutException {} (assoc op :type :info :error :timeout :node (:node this)))
      (catch Object {} (assoc op :type :info :error :unhandled-exception :node (:node this))))))

 (teardown! [_this _test])

 (close! [_this _test]))

(defn workload
  "Linearizable reads, writes, and compare-and-set operations on independent keys."
  [opts]
  {:client    (RegisterClient. (hu/connection-manager))
   :checker   (independent/checker
               (checker/compose
                {:linear   (checker/linearizable {:model     (model/cas-register)
                                                  :algorithm :linear})}))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_k]
                 (->> (gen/mix [r w cas])
                      (gen/limit (:ops-per-key opts)))))})
