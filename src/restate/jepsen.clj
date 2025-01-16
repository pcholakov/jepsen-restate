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
   [cheshire.core :as json]
   [slingshot.slingshot :refer [try+]]
   [knossos.model :as model]))

(def resources-relative-path ".")
(def server-restate-root "/opt/restate/")
(def server-restate-binary "restate-server")
(def server-logfile (str server-restate-root "/restate.log"))
(def server-pidfile (str server-restate-root "/restate.pid"))
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
      (info node "Setting up Restate Server" version "for testing")
      (c/su
       (c/exec :apt :install :-y :docker.io :nodejs :npm)

       (c/exec :mkdir :-p "/opt/services")
       (c/upload (str resources-relative-path "/services/dist/services.zip") "/opt/services.zip")
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
             metadata-store-address (str "http://" (first (:nodes test)) ":5122/")]
         (info "Starting node `" node-name "` with: " :--node-name (str "n" (inc (.indexOf (:nodes test) node)))
               :--force-node-id node-id
               :--allow-bootstrap (if (= node (first (:nodes test))) "true" "false")
               :--metadata-store-address metadata-store-address)
         (c/exec
          :docker
          :run
          :--name=restate
          :--network=host
          :--add-host :host.docker.internal:host-gateway
          :--detach
          :--volume "/opt/config.toml:/config.toml"
          :--volume "/opt/restate/restate-data:/restate-data"
          :--env (str "RESTATE_METADATA_STORE_CLIENT__ADDRESS=" metadata-store-address)
          (:image test)
          :--node-name node-name
          :--force-node-id node-id
          :--allow-bootstrap (if (= node (first (:nodes test))) "true" "false")
          :--auto-provision-partitions (if (= node (first (:nodes test))) "true" "false")
          :--config-file "/config.toml"
          ;;:--metadata-store-address metadata-store-address
          ))
       (cu/await-tcp-port 9070)
       (cu/await-tcp-port 8080)

       (when (= node (first (:nodes test)))
         (info "Registering Restate service on first node")
         (c/exec :npx "@restatedev/restate" :deployments :register "http://host.docker.internal:9080" :--yes))))

    (teardown! [_this _test node]
      (info node "Tearing down Restate")
      (c/su
       (cu/stop-daemon! server-restate-binary server-pidfile)
       (c/exec :rm :-rf server-restate-root)
       (c/exec :rm :-rf server-services-dir)
       (cu/stop-daemon! node-binary services-pidfile)))

    db/LogFiles (log-files [_this _test _node] [server-logfile services-logfile])))

(defn parse-long-nil
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (parse-long s)))

(defrecord Client [ingress-url] client/Client
           (setup! [_this _test])

           (open! [this _test node]
             (assoc this :ingress-url (str "http://" node ":8080")))

           (invoke! [_client _test op]
             (let [[k v] (:value op)]
               (try+
                (case (:f op)
                  :read (let [value
                              (->> (http/get (str ingress-url "/Register/" k "/get"))
                                   (:body)
                                   (parse-long-nil))]
                          (assoc op :type :ok, :value (independent/tuple k value)))

                  :write (do (http/post (str ingress-url "/Register/" k "/set")
                                        {:body (json/generate-string v)
                                         :content-type :json})
                             (assoc op :type :ok))

                  :cas (let [[old new] v]
                         (http/post (str ingress-url "/Register/" k "/cas")
                                    {:body (json/generate-string {:expected old :newValue new})
                                     :content-type :json})
                         (assoc op :type :ok)))

                (catch [:status 412] _
                  (assoc op
                         :type  :fail
                         :error :precondition-failed)))))

           (teardown! [_this _test])

           (close! [_this _test]))

(defn r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

(defn register-workload
  "Linearizable reads, writes, and compare-and-set operations on independent keys."
  [opts]
  {:client    (Client. nil)
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
  {"register" register-workload})

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
           {:pure-generators true
            :name            (str "restate " (name (:workload opts)))
            :os              debian/os
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
