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

(defrecord ZaumSQLite [connection]
  z/IZaumDatabase
  (perform-get [_ {:keys [entity identifier]}]
    (cond
      (nil? identifier)
      (jdbc/query @connection [(str "SELECT * from " entity)])
      (map? identifier)
      (vec (filter (construct-filter identifier) (@connection entity))))))

;;TODO: this is a stop-gap - eventually we need to use the connection map
(defn new-sqlite
  [connection-map]
  (ZaumSQLite. (atom connection-map)))
