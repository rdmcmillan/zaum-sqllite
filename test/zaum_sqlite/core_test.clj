(ns zaum-sqlite.core-test
  (:require [clojure.test :refer :all]
            [zaum.core :as z]
            [zaum-sqlite.core :refer :all]
            [clojure.pprint]))

(def test-data
  {:table-0
   [{:created-at 0 :updated-at 1 :text "foo" :other-val 4}
    {:created-at 1 :updated-at 1 :text "foo" :other-val 7}
    {:created-at 2 :updated-at 2 :text "bar" :other-val 5}
    {:created-at 3 :updated-at 3 :text "baz" :other-val 6}]})

(def test-table-zero (-> test-data
                         keys
                         first))

(def test-con
  {:subprotocol "sqlite"
   :subname "test.db"
   :dbtype :sqlite})

;;TODO hammock time:
;; 1) ddl as maps
;; 2) ddl inferred from a col schema(?)
;; 3) sane migrations, ie as above and complement of roll-up is roll-down, etc.
(def test-ddl [[:created-at :int]
               [:updated-at :int]
               [:text "varchar(10)"]
               [:other-val :int]])

(deftest test-kebab-to-snake
  (testing "Change kebab case string to snake case."
    (is (= "naming_things_is_hard"
           (kebab-to-snake "naming-things-is-hard"))))
  (testing "Change kebab case keyword to snake case."
    (is (= :naming_things_is_hard
           (kebab-to-snake :naming-things-is-hard))))
  (testing "Wrong type past to kebab-to-snake"
    (let [result (try (kebab-to-snake 1)
                      (catch Throwable t t))]
      (is (= (:cause (Throwable->map result))
             "Wrong type: class java.lang.Long passed to kebab-to-snake")))))

(deftest test-snake-to-kebab
  (testing "Change snake case string to kebab case."
    (is (= "naming-things-is-hard"
           (snake-to-kebab "naming_things_is_hard"))))
  (testing "Change snake case keyword to kebab case."
    (is (= :naming-things-is-hard
           (snake-to-kebab :naming_things_is_hard))))
  (testing "Wrong type past to snake-to-kebab"
    (let [result (try (snake-to-kebab 1)
                      (catch Throwable t t))]
      (is (= (:cause (Throwable->map result))
             "Wrong type: class java.lang.Long passed to snake-to-kebab")))))

;; TODO fixture to rm test db after

(deftest test-database-level-operations
  (testing "Create a table..."
    (let [result (z/perform-op
                   :create
                   {:operation   :create
                    :connection  (z/init-connection test-con)
                    :level       :table
                    :entity      test-table-zero
                    :column-ddl  test-ddl})]
      (is (= (:status result) :ok))))
  (testing "Create catch for duplicate table creation attempt..."
    (let [result (z/perform-op
                   :create
                   {:operation   :create
                    :connection  (z/init-connection test-con)
                    :level       :table
                    :entity      test-table-zero
                    :column-ddl  test-ddl})]
      (is (= (:status result) :error))
      (is (= (:cause (Throwable->map (:data result))) "Attempt to create duplicate table.")))))

(deftest test-select-empty-rows
  (let [result (z/perform-op
                 :read
                 {:operation   :read
                  :connection  (z/init-connection test-con)
                  :level       :table
                  :entity      test-table-zero})]
    (is (= :ok (:status result)))
    (is (= 0 (:count result)))
    (is (= [] (:data result)))))

(deftest test-create-single-rows
  (testing "Basic test for creating sqlite row data."
    (let [create (z/process-command {:operation  :create
                                     :connection (z/init-connection test-con)
                                     :level      :row
                                     :entity     test-table-zero
                                     :data       (first (test-table-zero test-data))})
          read (z/process-command {:operation  :read
                                   :connection (z/init-connection test-con)
                                   :entity     test-table-zero})]
      (is (= :ok (:status create)))
      (is (= 1 (:count create)))
      (is (= (:count create) (count (:data create))))
      (is (= (str "Row in " test-table-zero " created.") (:message create)))
      (is (= :ok (:status read)))
      (is (= 1 (:count read)))
      (is (= 0 (get-in read [:data 0 :created-at])))
      (is (= 1 (get-in read [:data 0 :updated-at])))
      (is (= "foo" (get-in read [:data 0 :text])))
      (is (= 4 (get-in read [:data 0 :other-val]))))))

#_(deftest test-create-multiple-rows
  (testing "Basic test for creating sqlite row data."
    (let [create (z/process-command {:operation  :create
                                     :connection (z/init-connection test-con)
                                     :level      :row
                                     :entity     test-table-zero
                                     :data       (rest (test-table-zero test-data))})
          read (z/process-command {:operation  :read
                                   :connection (z/init-connection test-con)
                                   :entity     test-table-zero})]
      (is (= 1 create))
      (is (= :ok (:status create)))
      (is (= 1 (:count create)))
      (is (= (:count create) (count (:data create))))
      (is (= "Rows in :table-0 created." (:message create)))
      (is (= :ok (:status read)))
      (is (= 1 (:count read))))))

#_(deftest test-get-all
  (testing "Basic test of getting all records in a table"
    (let [result (z/process-command
                   {:operation  :read
                    :connection (z/init-connection test-con)
                    :entity     test-table-zero})]
      (is (= :ok (:status result)))
      (is (= (:count result) (count (:data result)))))))

(deftest crud
  (test-database-level-operations)
  (test-select-empty-rows)
  (test-create-single-rows)
  #_(test-create-multiple-rows)
  #_(test-get-all))

(defn test-ns-hook
  []
  (test-kebab-to-snake)
  (test-snake-to-kebab)
  (crud))

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
