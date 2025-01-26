(ns restate.jepsen
  (:require
   [clojure.tools.logging :refer [info]]
   [clojure.math :as m]
   [clojure.string :as str]
   [jepsen
    [checker :as checker]
    [cli :as cli]
    [client :as client]
    [control :as c]
    [db :as db]
    [generator :as gen]
    [independent :as independent]
    [nemesis :as nemesis]
    [tests :as tests]]
   [jepsen.checker.timeline :as timeline]
   [jepsen.control.util :as cu]
   [jepsen.os.debian :as debian]
   [clj-http.client :as http]
   [clj-http.conn-mgr :as conn-mgr]
   [cheshire.core :as json]
   [slingshot.slingshot :refer [try+]]
   [knossos.model :as model]
   [restate.util :as ru]
   [restate.jepsen.set :as mds-set]))

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
  [opts]
  (reify db/DB
    (setup! [_db test node]
      (when (not (:dummy? (:ssh test)))
        (info node "Setting up Restate")
        (c/su
         (c/exec :apt :install :-y :docker.io :nodejs :jq)

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
            :--env (str "RESTATE_BOOTSTRAP_NUM_PARTITIONS=" (:num-partitions opts))
            :--env (str "RESTATE_METADATA_STORE_CLIENT__ADDRESSES=" metadata-store-address-list)
            :--env (str "RESTATE_ADVERTISED_ADDRESS=http://" node ":5122")
            :--env (str "RESTATE_BIFROST__REPLICATED_LOGLET__DEFAULT_REPLICATION_PROPERTY={node: "
                        (->> (/ (count (:nodes test)) 2) m/floor int inc) "}")
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

         (info "Waiting for all nodes to join cluster and partitions to be configured")
         (ru/wait-for-metadata-servers (count (:nodes test)))
         (ru/wait-for-logs (:num-partitions opts))
         (ru/wait-for-partition-leaders (:num-partitions opts))
         (ru/wait-for-partition-followers (* (:num-partitions opts) (dec (count (:nodes test)))))

         (when (= node (first (:nodes test)))
           (info "Performing once-off setup")
           (ru/restate :deployments :register "http://host.docker.internal:9080" :--yes)
            ;; (when (> (count (:nodes test)) 2)
            ;;   (let [replication-factor (int (+ 1 (m/floor (/ (count (:nodes test)) 2))))]
            ;;     (info "Reconfiguring all logs with replication factor:" replication-factor)
            ;;     (doseq [log-id (range (:num-partitions opts))]
            ;;       (ru/restatectl :logs :reconfigure
            ;;                      :--log-id log-id :--provider :replicated
            ;;                      :--replication-factor-nodes replication-factor))))
           )

         (info "Waiting for service deployment")
         (ru/wait-for-deployment))))

    (teardown! [_this test node]
      (when (not (:dummy? (:ssh test)))
        (info node "Tearing down Restate")
        (c/su
         (c/exec :rm :-rf server-restate-root)
         (c/exec :rm :-rf server-services-dir)
         (c/exec :docker :rm :-f "restate")
         (cu/stop-daemon! node-binary services-pidfile))))

    db/LogFiles (log-files [_this test _node]
                  (when (not (:dummy? (:ssh test)))
                    (c/su (c/exec* "docker logs restate" "&>" server-logfile)
                          (c/exec :chmod :644 server-logfile))
                    [server-logfile services-logfile]))))

(defn parse-long-nil
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (parse-long s)))

(defrecord
 RegisterServiceClient [conn-mgr] client/Client

 (setup! [_this _test])

 (open! [this test node] (assoc this
                                :node (str "n" (inc (.indexOf (:nodes test) node)))
                                :ingress-url (str "http://" node ":8080")))

 (invoke! [this _test op]
   (let [[k v] (:value op)]
     (try+
      (case (:f op)
        :read (let [value
                    (->> (http/get (str (:ingress-url this) "/Register/" k "/get")
                                   {:connection-manager conn-mgr})
                         (:body)
                         (parse-long-nil))]
                (assoc op :type :ok :value (independent/tuple k value) :node (:node this)))

        :write (do (http/post (str (:ingress-url this) "/Register/" k "/set")
                              {:body (json/generate-string v)
                               :content-type :json
                               :connection-manager conn-mgr})
                   (assoc op :type :ok :node (:node this)))

        :cas (let [[old new] v]
               (http/post (str (:ingress-url this) "/Register/" k "/cas")
                          {:body (json/generate-string {:expected old :newValue new})
                           :content-type :json
                           :connection-manager conn-mgr})
               (assoc op :type :ok :node (:node this))))

      (catch [:status 412] {} (assoc op :type :fail :error :precondition-failed :node (:node this)))

      ;; for the CAS service, a 404 means deployment hasn't yet completed -
      ;; this is likely replication latency
      (catch [:status 404] {} (assoc op :type :fail :error :not-found :node (:node this)))

      (catch java.net.ConnectException {} (assoc op :type :fail :error :connect :node (:node this)))

      (catch [:status 500] {} (assoc op :type :info :info :server-internal :node (:node this)))
      (catch java.net.SocketTimeoutException {} (assoc op :type :info :error :timeout :node (:node this)))
      (catch Object {} (assoc op :type :info)))))

 (teardown! [_this _test])

 (close! [_this _test]))

(defrecord
 MetadataStoreServiceClient [conn-mgr] client/Client

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
  "A map of workloads."
  {"register" register-workload
   "register-mds" register-mds-workload
   "set-mds" mds-set/workload})

(def nemeses
  "A map of nemeses."
  {"none" nil
   "container-killer"      (nemesis/node-start-stopper
                            rand-nth
                            (fn start [_t _n]
                              (c/su (c/exec :docker :kill :-s :KILL "restate"))
                              [:killed "restate-server"])
                            (fn stop [_t _n]
                              (c/su (c/exec :docker :start "restate"))
                              [:restarted "restate-server"]))
   "partition-random-node" (nemesis/partition-random-node)})

(defn restate-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key.
      :workload     Type of workload.
      :nemesis      Nemesis to apply."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           (if (not (:dummy? (:ssh opts))) {:os debian/os} nil)
           {:pure-generators true
            :name            (str "restate-" (name (:workload opts)))
            :db              (restate {:num-partitions 3})
            :client          (:client workload)
            :nemesis         (get nemeses (:nemesis opts))
            :generator       (gen/phases
                              (->> (:generator workload)
                                   (gen/stagger (/ (:rate opts)))
                                   (gen/nemesis (cycle [(gen/sleep 5)
                                                        {:type :info, :f :start}
                                                        (gen/sleep 5)
                                                        {:type :info, :f :stop}]))
                                   (gen/time-limit (:time-limit opts)))
                              (gen/nemesis [{:type :info, :f :stop}])
                              (->>
                               (gen/clients (:generator workload))
                               (gen/stagger (/ (:rate opts)))
                               (gen/time-limit 5)))
            :checker         (checker/compose
                              {:perf       (checker/perf)
                               :stats      (checker/stats)
                               :exceptions (checker/unhandled-exceptions)
                               :workload   (:checker workload)})})))

(def cli-opts
  "Additional command line options."
  [["-i" "--image STRING" "Restate container version"
    :default "ghcr.io/restatedev/restate:main"]
   [nil "--image-tarball STRING" "Restate container local path"]
   ["-w" "--workload NAME" "Workload to run"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate (workloads (cli/one-of workloads))]
   ["-N" "--nemesis NAME" "Nemesis to apply"
    :default "none"
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
