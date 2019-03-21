(ns onyx.plugin.test-datomic
  (:require [onyx.plugin.datomic :as sut]
            [clojure.test :refer :all]))

(def test-ex1
  (ex-info "Transaction timeout"
           {:db/error :db.error/transaction-timeout}))

(def test-ex2
  (Exception. "Transactor Timeout"
              (ex-info "Transactor Unavailable"
                       {:db/error :db.error/transactor-unavailable})))

(def test-ex-other1
  (ex-info "Other Exception"
           {:db/error :db.error/unique-conflict}))

(def test-ex-other2
  (Exception. "Other Exception 2" test-ex-other1))

(deftest test-get-exception-cause
  (is (= :db.error/transaction-timeout (sut/get-exception-cause test-ex1)))
  (is (= :db.error/transactor-unavailable (sut/get-exception-cause test-ex2))))

(deftest test-try-deref
  (is (= ::sut/restartable-ex (sut/try-deref (future (throw test-ex1)))))
  (is (= ::sut/restartable-ex (sut/try-deref (future (throw test-ex2)))))
  (is (thrown-with-msg? Exception
                        #"Unrecoverable transaction"
                        (sut/try-deref (future (throw test-ex-other1)))))
  (is (thrown-with-msg? Exception
                        #"Unrecoverable transaction"
                        (sut/try-deref (future (throw test-ex-other2))))))





