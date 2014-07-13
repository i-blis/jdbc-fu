(ns jdbc-fu.info
  (:require [clojure.java.jdbc :as jdbc])
  (:import java.sql.DatabaseMetaData))

(defn tables
  "Retrieves a description of the tables (and views) available in the given catalog.
  If no catalog name (as string) is provided, the default one(s) supported by the 
  database, which may mean none at all, are used. Optionaly accepts schema and table name patterns (as strings) and 
  types (as a string vector) to limit results. Returns a sequence of raw maps as
  yielded by java.jdbc (albeit with keywordised keys), unless fns to be applied to 
  map rows or key identifiers are provided."
  [db-spec & {:keys [catalog schema table types
                     row-fn identifiers as-arrays?]
              :or   {types ["TABLE" "VIEW"]
                     row-fn identity
                     identifiers identity}}])
(jdbc/with-db-metadata [db-meta db-spec]
  (-> db-meta
      (.getTables catalog schema table (into-array String types))
      (jdbc/metadata-result :row-fn row-fn :as-arrays? as-arrays? :identifiers identifiers)))
