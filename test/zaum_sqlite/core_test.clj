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

(def test-con
  {:class-name "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname "test.db"
   :dbtype :sqlite})

(def test-ddl [[:thing1 "int" :primary :key]])

;; TODO fixture to rm test db after

(deftest test-database-level-operations
  (testing "Create a table..."
    (let [result (z/perform-op
                   :create
                   {:operation   :create
                    :connection  (z/init-connection test-con)
                    :level       :table
                    :entity      :test
                    :column-ddl  test-ddl})]
      (is (= (:status result) :ok))))
  (testing "Create catch for duplicate table creation attempt..."
    (let [result (z/perform-op
                   :create
                   {:operation   :create
                    :connection  (z/init-connection test-con)
                    :level       :table
                    :entity      :test
                    :column-ddl  test-ddl})]
      (is (= (:status result) :error))
      (is (= (:cause (Throwable->map (:data result))) "Attempt to create duplicate table.")))))

#_(deftest test-create-table
  (testing "Basic test for creating a sqlite table"
    (let [con (new-sqlite {:class-name  "org.sqlite.JDBC"
                              :subprotocol "sqlite"
                              :db          "test.db"})
          ent :test-table-created
          create (z/process-command {:operation  :create
                                     :connection @(:connection con)
                                     :level      :table
                                     :entity     ent})
          read (z/process-command {:operation :read
                                   :connection con
                                   :entity ent})]
      (is (= :ok (:status create)))
      (is (= 1 (:count create)))
      (is (= (:count create) (count (:data create))))
      (is (= "Table :table-0 created." (:message create)))
      (is (= :ok (:status read)))
      (is (zero? (:count read))))))

#_(deftest test-get-all
  (testing "Basic test of getting all records in a table"
    (let [result (z/process-command
                               {:operation  :read
                                ;;TODO: how do our connection strings work?
                                :connection {:dbtype "sqlite"
                                             :dbname "test.db"
                                             :impl
                                             (new-sqlite {:dbtype "sqlite"
                                                          :dbname "test.db"})}
                                :entity     "test"})]
      (is (= :ok (:status result)))
      (is (= (:count result) (count (:data result)))))))

#_(deftest test-get-by-identifier
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

#_(deftest test-greater-than
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
