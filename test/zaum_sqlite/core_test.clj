(ns zaum-sqlite.core-test
  (:require [clojure.test :refer :all]
            [zaum.core :as z]
            [zaum-sqlite.core :refer :all]))

(def test-data
  {:table-0
   [{:created-at 0 :updated-at 1 :text "foo" :other-val 4}
    {:created-at 1 :updated-at 1 :text "foo" :other-val 7}
    {:created-at 2 :updated-at 2 :text "bar" :other-val 5}
    {:created-at 3 :updated-at 3 :text "baz" :other-val 6}]})

(deftest test-get-all
  (testing "Basic test of getting all records in a table"
    (let [result (z/perform-op :get
                               {:operation  :get
                                ;;TODO: how do our connection strings work?
                                :connection {:dbtype "sqlite"
                                             :dbname "test.db"
                                             :impl
                                             (new-sqlite {:dbtype "sqlite"
                                                          :dbname "test.db"})}
                                :entity     "test"})]
      (is (= :ok (:status result)))
      (is (= (:count result) (count (:data result)))))))

(deftest test-get-by-identifier
  (testing "Test getting one entry by an identifier one key"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:created-at 0}})]
      (is (= :ok (:status result)))
      (is (= (:count result)) (count (:data result)))
      (is (= (get-in result [:data 0 :other-val]) 4))
      (is (= (:count result) 1))))
  (testing "Test getting one entry by an identifier w/ multiple keys"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:updated-at 1
                                             :created-at 1}})]
      (is (= :ok (:status result)))
      (is (= (:count result)) (count (:data result)))
      (is (= (get-in result [:data 0 :other-val]) 7))
      (is (= (:count result) 1))))
  (testing "Test getting multiple entries by an identifier w/ multiple keys"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:text       "foo"
                                             :updated-at 1}})]
      (is (= :ok (:status result)))
      (is (= (:count result)) (count (:data result)))
      (is (= (:count result) 2))))
  (testing "Test getting no results for non-matching identifier clause"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:updated-at 10}})]
      (is (= :ok (:status result)))
      (is (= (:count result)) (count (:data result)))
      (is (zero? (:count result))))))

(deftest test-greater-than
  (testing "test basic > performance in memory"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:updated-at [> 1]}})]
      (is (= :ok (:status result)))
      (is (= (:count result) 2))
      (is (= (:count result) (count (:data result))))
      (is (= "bar" (-> result :data first :text)))
      (is (= "baz" (-> result :data second :text)))))
  (testing "test basic :> performance in memory"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:updated-at [:> 1]}})]
      (is (= :ok (:status result)))
      (is (= (:count result) 2))
      (is (= (:count result) (count (:data result))))
      (is (= "bar" (-> result :data first :text)))
      (is (= "baz" (-> result :data second :text)))))
  (testing "test :> performance in memory returning nothing"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:updated-at [:> 3]}})]
      (is (= :ok (:status result)))
      (is (zero? (:count result)))
      (is (= (:count result) (count (:data result))))))
  (testing "test :> performance in memory returning across two keys"
    (let [result (z/perform-op :get
                               {:operation  :get
                                :connection {:impl (new-sqlite test-data)}
                                :entity     :table-0
                                :identifier {:updated-at [:> 1]
                                             :other-val  [:> 5]}})]
      (is (= :ok (:status result)))
      (is (= (:count result) 1))
      (is (= (:count result) (count (:data result))))
      (is (= "baz" (-> result :data first :text))))))
