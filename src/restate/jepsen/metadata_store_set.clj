(ns restate.jepsen.metadata-store-set
  "A set client implemented on top of the Restate Metadata Store HTTP API.
  Note: restate-server must be compiled with the metadata-api feature."
  (:require
   [jepsen [client :as client]
    [checker :as checker]
    [generator :as gen]]
   [clj-http.client :as http]
   [cheshire.core :as json]
   [slingshot.slingshot :refer [try+]]
   [restate [http :as hu]]))

(defrecord
 SetClient [key conn-mgr] client/Client

 (open! [this test node]
   (assoc this
          :node (str "n" (inc (.indexOf (:nodes test) node)))
          :endpoint (str "http://" node ":9070/metadata/")
          :defaults {:connection-manager conn-mgr
                     :connection-timeout 500
                     :socket-timeout 1000}
          :random (new java.util.Random)))

 (setup! [this _test]
   (http/put (str (:endpoint this) key)
             (merge (:defaults this)
                    {:body (json/generate-string #{})
                     :headers {:If-Match "*" :ETag 1}
                     :content-type :json})))

 (invoke! [this _test op]
   (case (:f op)
     :read (assoc op
                  :type :ok,
                  :value (->> (http/get (str (:endpoint this) key)
                                        (:defaults this))
                              (:body)
                              (json/parse-string)
                              set))

     :add (try+
           (let [[new-set stored-version]
                 (let [res (http/get (str (:endpoint this) key)
                                     (:defaults this))]
                   [(conj (->> (json/parse-string (:body res)) set) (:value op))
                    (parse-long (->> res (:headers) (:ETag)))])]
             (http/put (str (:endpoint this) key)
                       (merge (:defaults this)
                              {:body (json/generate-string new-set)
                               :headers {:If-Match stored-version
                                         :ETag (inc stored-version)}
                               :content-type :json}))
             (assoc op :type :ok))
           (catch [:status 412] {} (assoc op :type :fail :error :precondition-failed :node (:node this)))
           (catch java.net.SocketTimeoutException {} (assoc op :type :info :error :timeout :node (:node this)))
           (catch Object {} (assoc op :type :info :error :unhandled-exception :node (:node this))))))

 (teardown! [_ _test])

 (close! [_ _test]))

(defn w
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))))

(defn r
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  "Restate Metadata Store-backed Set test workload."
  [_opts]
  {:client    (SetClient. "jepsen-set" (hu/connection-manager))
   :checker   (checker/set-full {:linearizable? true})
   :generator (gen/reserve 5 (repeat (r)) (w))})
