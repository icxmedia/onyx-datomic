(ns onyx.plugin.input-index-range-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.datomic]
            [onyx.api]
            [midje.sweet :refer :all]
            [datomic.api :as d]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :netty
   :onyx.messaging/peer-port-range [40200 40400]
   :onyx.messaging/peer-ports [40199]
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/backpressure-strategy :high-restart-latency
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/index true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def conn (d/connect db-uri))

@(d/transact conn schema)

(def people
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Derek"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Kristen"}])

@(d/transact conn people)

(def db (d/db conn))

(def t (d/next-t db))

(def batch-size 20)

(def out-chan (chan 1000))

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(def workflow
  [[:read-index-datoms :persist]])

(def catalog
  [{:onyx/name :read-index-datoms
    :onyx/plugin :onyx.plugin.datomic/read-index-range
    :onyx/type :input
    :onyx/medium :datomic
    :datomic/uri db-uri
    :datomic/t t
    :datomic/index-attribute :user/name
    :datomic/index-range-start "Benti"
    :datomic/index-range-end "Kristen"
    :datomic/datoms-per-segment 20
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads a range of datoms from the d/index-range API"}

   {:onyx/name :persist
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(defn inject-persist-ch [event lifecycle]
  {:core.async/chan out-chan})

(def persist-calls
  {:lifecycle/before-task-start inject-persist-ch})


(def lifecycles
  [{:lifecycle/task :read-index-datoms
    :lifecycle/calls :onyx.plugin.datomic/read-index-range-calls}
   {:lifecycle/task :persist
    :lifecycle/calls ::persist-calls}
   {:lifecycle/task :persist
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(fact (set (map #(nth % 2) (mapcat :datoms results)))
      => #{"Benti" "Derek" "Dorrene"})

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)