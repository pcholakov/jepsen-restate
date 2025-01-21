(ns restate.jepsen
  (:require
   [clojure.tools.logging :refer :all]
   [clojure.string :as str]
   [jepsen
    [checker :as checker]
    [cli :as cli]
    [client :as client]
    [control :as c]
    [db :as db]
    [generator :as gen]
    [independent :as independent]
    [tests :as tests]]
   [jepsen.checker.timeline :as timeline]
   [jepsen.control.util :as cu]
   [jepsen.os.debian :as debian]
   [clj-http.client :as http]
   [clj-http.conn-mgr :as conn-mgr]
   [cheshire.core :as json]
   [slingshot.slingshot :refer [try+]]
   [knossos.model :as model]))

(def resources-relative-path ".")
(def server-restate-root "/opt/restate/")
(def server-logfile (str server-restate-root "/restate.log"))
(def server-services-dir "/opt/services/")
(def node-binary "/usr/bin/node")
(def services-args (str server-services-dir "services.js"))
(def services-pidfile (str server-services-dir "services.pid"))
(def services-logfile (str server-services-dir "services.log"))

(defn restate
  "A deployment of Restate along with the required test services."
  [version]
  (reify db/DB
    (setup! [_db test node]
      (when (not (:dummy? (:ssh test)))
        (info node "Setting up Restate Server" version "for testing")
        (c/su
         (c/exec :apt :install :-y :docker.io :nodejs :npm)

         (c/exec :mkdir :-p "/opt/services")
         (c/upload (str resources-relative-path "/services/dist/services.zip") "/opt/services.zip")
         (when (:image-tarball test)
           (c/upload (:image-tarball test) "/opt/restate.tar")
           (c/exec :docker :load :--input "/opt/restate.tar")
           (c/exec :docker :tag (:image test) "restate"))
         (cu/install-archive! "file:///opt/services.zip" "/opt/services")
         (c/exec :rm "/opt/services.zip")

         (cu/start-daemon!
          {:logfile services-logfile
           :pidfile services-pidfile
           :chdir   "/opt/services"}
          node-binary services-args)
         (cu/await-tcp-port 9080)

         (c/exec :docker :rm :-f "restate")

         (c/upload (str resources-relative-path "/config/config.toml") "/opt/config.toml")
         (let [node-name (str "n" (inc (.indexOf (:nodes test) node)))
               node-id (inc (.indexOf (:nodes test) node))
               metadata-store-address-list (str "[" (str/join "," (map (fn [n] (str "http://" n ":5122")) (:nodes test))) "]")]
           (c/exec
            :docker
            :run
            :--name=restate
            :--network=host
            :--add-host :host.docker.internal:host-gateway
            :--detach
            :--volume "/opt/config.toml:/config.toml"
            :--volume "/opt/restate/restate-data:/restate-data"
            :--env (str "RESTATE_METADATA_STORE_CLIENT__ADDRESSES=" metadata-store-address-list)
            :--env (str "RESTATE_ADVERTISED_ADDRESS=http://" node ":5122")
            :--env "DO_NOT_TRACK=true"
            (:image test)
            :--node-name node-name
            :--force-node-id node-id
            :--allow-bootstrap (if (= node (first (:nodes test))) "true" "false")
            :--auto-provision-partitions (if (= node (first (:nodes test))) "true" "false")
            :--config-file "/config.toml"
            ;; :--metadata-store-address metadata-store-address ;; TODO: this doesn't seem to have an effect
            ;; :--advertise-address (str "http://" node ":5122") ;; TODO: this doesn't seem to have an effect
            ))
         (cu/await-tcp-port 9070)

         (when (= node (first (:nodes test)))
           (info "Registering Restate service on first node")
           (c/exec :npx "@restatedev/restate" :deployments :register "http://host.docker.internal:9080" :--yes))

         ;; HACK! make ourselves a candidate by positional index; timing-dependent and flaky
         (let [node-idx (.indexOf (:nodes test) node)]
           (when (not= node-idx 0)
             (info "Marking ourselves a MDS OP node set candidate member ->" node-idx)
             (Thread/sleep (* 1000 node-idx))
             ;; restatectl metadata patch --key nodes_config --patch '[{ "op": "replace", "path": "/nodes/${node_index}/1/Node/metadata_server_config/metadata_server_state", "value": "candidate" }]'
             (c/exec :docker :exec :restate :restatectl :metadata :patch
                     "--key" "nodes_config"
                     "--patch" (str "[{ \"op\": \"replace\", \"path\": \"/nodes/" node-idx
                                    "/1/Node/metadata_server_config/metadata_server_state\", \"value\": \"candidate\" }]"))))

          ;; TODO: replace with wait-for-healthy, not just listening
         (cu/await-tcp-port 8080))))

    (teardown! [_this test node]
      (when (not (:dummy? (:ssh test)))
        (info node "Tearing down Restate")
        (c/su
         (c/exec :rm :-rf server-restate-root)
         (c/exec :rm :-rf server-services-dir)
         (c/exec :docker :rm :-f "restate")
         (cu/stop-daemon! node-binary services-pidfile))))

    db/LogFiles (log-files [_this _test _node]
                  (c/su (c/exec :docker :logs "restate" :> server-logfile)
                        (c/exec :chmod :644 server-logfile))
                  [server-logfile services-logfile])))

(defn parse-long-nil
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (parse-long s)))

(defrecord
 RegisterServiceClient [conn-mgr] client/Client

 (setup! [_this _test])

 (open! [this _test node] (assoc this :ingress-url (str "http://" node ":8080")))

 (invoke! [this _test op]
   (let [[k v] (:value op)]
     (try+
      (case (:f op)
        :read (let [value
                    (->> (http/get (str (:ingress-url this) "/Register/" k "/get")
                                   {:connection-manager conn-mgr})
                         (:body)
                         (parse-long-nil))]
                (assoc op :type :ok, :value (independent/tuple k value)))

        :write (do (http/post (str (:ingress-url this) "/Register/" k "/set")
                              {:body (json/generate-string v)
                               :content-type :json
                               :connection-manager conn-mgr})
                   (assoc op :type :ok))

        :cas (let [[old new] v]
               (http/post (str (:ingress-url this) "/Register/" k "/cas")
                          {:body (json/generate-string {:expected old :newValue new})
                           :content-type :json
                           :connection-manager conn-mgr})
               (assoc op :type :ok)))

      (catch [:status 412] {} (assoc op
                                     :type  :fail
                                     :error :precondition-failed))

      ;; for the CAS service, a 404 means deployment hasn't yet completed -
      ;; this is likely replication latency
      (catch [:status 404] {} (assoc op
                                     :type  :fail
                                     :error :not-found))

      (catch [:status 500] {} (assoc op
                                     :type  :fail
                                     :error :server-internal))

      (catch java.net.ConnectException {} (assoc op
                                                 :type  :fail
                                                 :error :timeout))

      (catch Object {} (assoc op
                              :type  :fail)))))

 (teardown! [_this _test])

 (close! [_this _test]))

(defrecord
 MetadataStoreServiceClient [conn-mgr] client/Client

 (setup! [_this _test])

 (open! [this _test node] (assoc this
                                 :admin-api (str "http://" node ":9070")
                                 :random (new java.util.Random)))

 (invoke! [this _test op]
   (let [[k v] (:value op)]
     (try+
      (case (:f op)
        :read (try+ (let [value (->> (http/get (str (:admin-api this) "/metadata/" k)
                                               {:connection-manager conn-mgr})
                                     (:body)
                                     (parse-long-nil))]
                      (assoc op :type :ok, :value (independent/tuple k value)))
                    (catch [:status 404] _
                      (assoc op :type :ok, :value (independent/tuple k nil))))
        :write (do
                 (http/put (str (:admin-api this) "/metadata/" k)
                           {:body (json/generate-string v)
                            :headers {:If-Match "*"
                                      :ETag (bit-shift-left (abs (.nextInt (:random this))) 1)}
                            :content-type :json
                            :connection-manager conn-mgr})
                 (assoc op :type :ok))

        :cas (let [[expected-value new-value] v
                   [stored-value stored-version]
                   (try+
                    (let [res (http/get (str (:admin-api this) "/metadata/" k)
                                        {:connection-manager conn-mgr})]
                      [(parse-long-nil (:body res)) (parse-long-nil (->> res (:headers) (:ETag)))])
                    (catch [:status 404] {} [nil nil]))]

               (if (= stored-value expected-value)
                 (do (http/put (str (:admin-api this) "/metadata/" k)
                               {:body (json/generate-string new-value)
                                :headers {:If-Match stored-version
                                          :ETag (inc stored-version)}
                                :content-type :json
                                :connection-manager conn-mgr})
                     (assoc op :type :ok))
                 (assoc op :type :fail :error :precondition-failed))))

      (catch [:status 412] {} (assoc op
                                     :type  :fail
                                     :error :precondition-failed))

      (catch [:status 404] {} (assoc op
                                     :type  :ok
                                     :value nil))

      (catch [:status 500] {} (assoc op
                                     :type  :fail
                                     :error :server-internal))

      (catch java.net.ConnectException {} (assoc op
                                                 :type  :fail
                                                 :error :timeout))

      (catch Object _ ((info (:throwable &throw-context) "Unhandled exception")
                       (assoc op
                              :type  :fail
                              :error :unhandled-exception))))))

 (teardown! [_this _test])

 (close! [_this _test]))

(defn r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

(defn http-connection-manager [& opts]
  (conn-mgr/make-reusable-conn-manager
   (merge opts {:timeout 2
                :default-per-route 20
                :threads 100})))

(defn register-workload
  "Linearizable reads, writes, and compare-and-set operations on independent keys."
  [opts]
  {:client    (RegisterServiceClient. (http-connection-manager))
   :checker   (independent/checker
               (checker/compose
                {:linear   (checker/linearizable {:model     (model/cas-register)
                                                  :algorithm :linear})
                 :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_k]
                 (->> (gen/mix [r w cas])
                      (gen/limit (:ops-per-key opts)))))})

(defn register-mds-workload
  "Linearizable reads, writes, and compare-and-set operations on independent keys."
  [opts]
  {:client    (MetadataStoreServiceClient. (http-connection-manager))
   :checker   (independent/checker
               (checker/compose
                {:linear   (checker/linearizable {:model     (model/cas-register)
                                                  :algorithm :linear})
                 :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_k]
                 (->> (gen/mix [r w cas])
                      (gen/limit (:ops-per-key opts)))))})

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"register" register-workload
   "register-mds" register-mds-workload})

(defn restate-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key.
      :workload     Type of workload."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           (if (not (:dummy? (:ssh opts))) {:os debian/os} nil)
           {:pure-generators true
            :name            (str "restate-" (name (:workload opts)))
            :db              (restate "v1.1.6")
            :client          (:client workload)
            :checker         (checker/compose
                              {:perf     (checker/perf)
                               :workload (:checker workload)})
            :generator       (gen/phases
                              (->> (:generator workload)
                                   (gen/stagger (/ (:rate opts)))
                                   (gen/time-limit (:time-limit opts)))
                              (gen/clients (:final-generator workload)))}
           {:client  (:client workload)
            :checker (:checker workload)})))

(def cli-opts
  "Additional command line options."
  [["-i" "--image STRING" "Restate container version"
    :default "ghcr.io/restatedev/restate:main"]
   [nil "--image-tarball STRING" "Restate container local path"]
   ["-w" "--workload NAME" "Workload to run"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate (workloads (cli/one-of workloads))]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(defn -main
  "CLI entry point. Start test or web server for viewing results."
  [& args]
  (cli/run!
   (merge
    (cli/single-test-cmd {:test-fn restate-test :opt-spec cli-opts})
    (cli/serve-cmd))
   args))
