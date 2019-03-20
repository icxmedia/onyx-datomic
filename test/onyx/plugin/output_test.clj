(ns onyx.plugin.output-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is]]
            [datomic.api :as d]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [write-datoms
                              write-bulk-tx-datoms
                              write-bulk-tx-datoms-async]]
             [core-async :as core-async]]))

(defn build-write-datoms-job [db-uri batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :identity]
                                    [:identity :out]]
                         :catalog [{:onyx/name :identity
                                    :onyx/fn :clojure.core/identity
                                    :onyx/type :function
                                    :onyx/batch-size batch-size}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (core-async/input :in batch-settings))
        (add-task (write-datoms :out (merge {:datomic/uri db-uri
                                             :datomic/partition :com.mdrogalis/people}
                                            batch-settings))))))

(defn build-write-bulk-datoms-job
  [db-uri batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :identity]
                                    [:identity :out]]
                         :catalog [{:onyx/name :identity
                                    :onyx/fn :clojure.core/identity
                                    :onyx/type :function
                                    :onyx/batch-size batch-size}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (core-async/input :in batch-settings))
        (add-task (write-bulk-tx-datoms :out (merge {:datomic/uri db-uri
                                                     :datomic/partition :com.mdrogalis/people}
                                                    batch-settings))))))

(defn build-write-bulk-datoms-async-job
  [db-uri batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :identity]
                                    [:identity :out]]
                         :catalog [{:onyx/name :identity
                                    :onyx/fn :clojure.core/identity
                                    :onyx/type :function
                                    :onyx/batch-size batch-size}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (core-async/input :in batch-settings))
        (add-task (write-bulk-tx-datoms-async
                   :out
                   (merge {:datomic/uri db-uri
                           :datomic/partition :com.mdrogalis/people}
                          batch-settings))))))

(defn ensure-datomic!
  ([db-uri data]
   (d/create-database db-uri)
   @(d/transact
     (d/connect db-uri)
     data)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def people
  [{:name "Mike"}
   {:name "Dorrene"}
   {:name "Benti"}
   {:name "Kristen"}
   {:name "Derek"}
   :done])

(def bulk-people
  [{:tx [[:db/add (d/tempid :com.mdrogalis/people) :name "Mike"]
         [:db/add (d/tempid :com.mdrogalis/people) :name "Dorrene"]]}
   {:tx [[:db/add (d/tempid :com.mdrogalis/people) :name "Benti"]
         [:db/add (d/tempid :com.mdrogalis/people) :name "Kristen"]]}
   {:tx [[:db/add (d/tempid :com.mdrogalis/people) :name "Derek"]]}
   :done])

(deftest write-datoms-tx-output-test
  (let [db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        job (build-write-datoms-job db-uri 10 1000)
        {:keys [in]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-datomic! db-uri schema)
        (pipe (spool people) in false)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (let [db (d/db (d/connect db-uri))]
          (is (= (set (apply concat (d/q '[:find ?a :where [_ :name ?a]] db)))
                 (set (remove nil? (map :name people)))))))
      (finally (d/delete-database db-uri)))))

(deftest write-bulk-datoms-tx-output-test
  (let [db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        _ (println bulk-people)
        {:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        job (build-write-bulk-datoms-job db-uri 10 1000)
        {:keys [in]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-datomic! db-uri schema)
        (pipe (spool bulk-people) in false)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (let [db (d/db (d/connect db-uri))]
          (is (= (set (apply concat (d/q '[:find ?a :where [_ :name ?a]] db)))
                 (set (remove nil? (map :name people)))))))
      (finally (d/delete-database db-uri)))))

(deftest write-bulk-datoms-tx-async-output-test
  (let [db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        job (build-write-bulk-datoms-async-job db-uri 10 1000)
        {:keys [in]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-datomic! db-uri schema)
        (pipe (spool bulk-people) in false)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (let [db (d/db (d/connect db-uri))]
          (is (= (set (apply concat (d/q '[:find ?a :where [_ :name ?a]] db)))
                 (set (remove nil? (map :name people)))))))
      (finally (d/delete-database db-uri)))))
