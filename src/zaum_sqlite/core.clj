(ns zaum-sqlite.core
  (:require [clojure.java.jdbc :as jdbc]
            [zaum.core :as z]))

(def search-types
  {>  >
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
         (jdbc/query connection
                     ["select * from sqlite_master where type ='table'"])))) ;; TODO use zaum query

(defn kebab-to-snake
  [kebab]
  (let [replace-dash #(clojure.string/replace % #"-" "_")]
    (cond
     (string? kebab) (replace-dash kebab)
     (keyword? kebab) (keyword (replace-dash (name kebab)))
     :or
     (throw (IllegalArgumentException.
              (str "Wrong type: " (type kebab) " passed to kebab-to-snake"))))))

(defn in-db-tables?
  [connection entity]
  (true? (some #(= (name (kebab-to-snake entity)) %) (db-tables connection))))


;; TODO Thinking we need level dispatch...
(defrecord ZaumSQLite [connection]
  z/IZaumDatabase
  (perform-create [_ {:keys [connection level entity data] :as command}]
    (cond
      (and (= level :table) (in-db-tables? connection entity))
      ;;TODO: likely an error condition or should it be idempotent and 'clean'?
      ;; - we're considering just returning this as an error
      ;; - not sure if it shouldn't be idempotent - we'll know more later
      (throw (Exception. "Attempt to create duplicate table."))
      (= level :table)
      ;;TODO: this represents the 'table' - not sure these implementations
      ;; shouldn't be adapted to assoc and return the command message
      {:status  :ok
       :data    (jdbc/db-do-commands connection
                                     (jdbc/create-table-ddl (kebab-to-snake entity)
                                                            (:column-ddl command)
                                                            {:entities kebab-to-snake}))
       :message (str "Table " entity " created.")}
      (= level :row)
      {:status :ok
       :data  (jdbc/insert! connection
                            (kebab-to-snake entity)
                            data
                            {:entities kebab-to-snake})
               :message (str "Rows in " entity " created.")}
      :or
      (throw (Exception. "Unknown create operation")))
    )
  (perform-read [_ {:keys [entity identifier]}]
    (let [result (vec
                   (jdbc/query connection
                               [(str "SELECT * from " (name (kebab-to-snake entity)))]))]
      {:status  :ok
       :data    result
       :message "read-value"})))

(defmethod z/prepare-connection :sqlite
  [connection-map]
  (ZaumSQLite. connection-map))