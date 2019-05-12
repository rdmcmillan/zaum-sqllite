(ns zaum-sqlite.core
  (:require [clojure.java.jdbc :as jdbc]
            [zaum.core :as z]))

(def search-types
  { > >
   :> >})

(defn- construct-filter
  [identifier]
  (fn [v]
    (every?
     (fn [filter-spec]
       (let [[filter-key filter-search] filter-spec]
         (and (contains? v filter-key)
              (cond (vector? filter-search)
                    ((search-types (first filter-search))
                     (filter-key v)
                     (second filter-search))
                    :or
                    (= (filter-key v) filter-search)))))
     identifier)))

(defn db-tables
  [connection]
  (vec
    (map #(:tbl_name %)
        (jdbc/query connection ["select * from sqlite_master where type='table'"])))) ;; TODO use zaum query

(defn in-db-tables?
  [connection entity]
  (true? (some #(= (name entity) %) (db-tables connection))))

(defrecord ZaumSQLite [connection]
  z/IZaumDatabase
  (perform-create [_ {:keys [connection level entity] :as command}]
    (cond
      (and (= level :table) (in-db-tables? connection entity))
           ;;TODO: likely an error condition or should it be idempotent and 'clean'?
           ;; - we're considering just returning this as an error
           ;; - not sure if it shouldn't be idempotent - we'll know more later
           (throw (Exception. "Attempt to create duplicate table."))
           (= level :table)
           ;;TODO: this represents the 'table' - not sure these implementations
           ;; shouldn't be adapted to assoc and return the command message
      (do
        (jdbc/db-do-commands connection
                             (jdbc/create-table-ddl entity (:column-ddl command)))
        ;; - for :data we return the empty table [] in a collection of "created" table(s)
        {:status :ok :data [[]] :message (str "Table " entity " created.")})
           :or
           (throw (Exception. "Unknown create operation"))))
  (perform-read [_ {:keys [entity identifier]}]
    (cond
      (nil? identifier)
      (jdbc/query @connection [(str "SELECT * from " entity)])
      (map? identifier)
      (vec (filter (construct-filter identifier) (@connection entity))))))

(defmethod z/prepare-connection :sqlite
  [connection-map]
  (ZaumSQLite. connection-map))