(ns jdbc-fu.info
  (:require [clojure.java.jdbc :as jdbc])
  (:import java.sql.DatabaseMetaData))

(defn tables
  "Retrieves a description of the tables (and views) available in the
  given catalog.
  If no catalog name is provided, the default one(s) supported by the
  database, which may mean none at all, is used. Optionaly accepts
  schema and table name patterns and table types to limit results.
  Returns a sequence of raw maps as yielded by java.jdbc (albeit with
  keywordised keys), unless fns to be applied to map rows or key
  identifiers are provided."
  [db-spec & {:keys [catalog schema table types row-fn
                     identifiers as-arrays?]
              :or   {types ["TABLE" "VIEW"]
                     row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getTables catalog schema table (into-array String types))
        (jdbc/metadata-result :row-fn row-fn
                              :identifiers identifiers
                              :as-arrays? as-arrays?))))

(defn columns
  "Retrieves a description of the columns available in the given
  catalog and table(s).
  If no catalog name or table name pattern is provided, the default
  one(s) supported by the database, which may mean none at all, is
  used. Accepts column and table name patterns to limit results.
  Returns a sequence of raw maps as yielded by java.jdbc (albeit with
  keywordised keys), unless fns to be applied to map rows or key
  identifiers are provided."
  [db-spec & {:keys [catalog schema table column row-fn identifiers as-arrays?]
              :or   {row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getColumns catalog schema table column)
        (jdbc/metadata-result :row-fn row-fn
                              :identifiers identifiers
                              :as-arrays? as-arrays?))))

(defn indexes
  "Retrieves a description of the indexes available in the given
  catalog and table(s). 
  If set to true, the unique? and approximate? keys allow for unique
  keys only and out of data values respectively. Returns a sequence of
  raw maps as yielded by java.jdbc (albeit with keywordised keys),
  unless fns to be applied to map rows or key identifiers are
  provided."
  [db-spec & {:keys [catalog schema table unique? approximate? row-fn identifiers as-arrays?]
              :or   {unique? false
                     approximate? false
                     row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getIndexInfo catalog schema table unique? approximate?)
        (jdbc/metadata-result :row-fn row-fn
                              :identifiers identifiers
                              :as-arrays? as-arrays?))))

(defn schemas
  "Retrieves the schemas available in the given catalog.
  Accepts a schema name pattern to filter results. Returns a sequence
  of raw maps as yielded by java.jdbc (albeit with keywordised keys),
  unless fns to be applied to map rows or key identifiers are
  provided."
  [db-spec & {:keys [catalog schema row-fn identifiers as-arrays?]
              :or   {row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getSchemas catalog schema)
        (jdbc/metadata-result :row-fn row-fn
                              :identifiers identifiers
                              :as-arrays? as-arrays?))))

(defn catalogs
  "Retrieves the available catalogs."
  [db-spec & {:keys [row-fn identifiers as-arrays?]
              :or   {row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getCatalogs)
        (jdbc/metadata-result :row-fn row-fn
                              :as-arrays? as-arrays?
                              :identifiers identifiers))))

(defn primary-keys
  "Retrieves a description of the given table's primary key columns.
  catalog and table(s).
  Returns a sequence of raw maps as yielded by java.jdbc (albeit with
  keywordised keys), unless fns to be applied to map rows or key
  identifiers are provided."
  [db-spec & {:keys [catalog schema table row-fn identifiers as-arrays?]
              :or   {row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getPrimaryKeys catalog schema table)
        (jdbc/metadata-result :row-fn row-fn
                              :identifiers identifiers
                              :as-arrays? as-arrays?))))

(def ^:private scopes
  {:temporary DatabaseMetaData/bestRowTemporary
   :transaction DatabaseMetaData/bestRowTransaction
   :session DatabaseMetaData/bestRowSession})

(defn best-row-identifiers
  "Retrieves a description of a table's optimal set of columns that
  uniquely identifies a row. Returns a sequence of raw maps as yielded
  by java.jdbc (albeit with keywordised keys), unless fns to be
  applied to map rows or key identifiers are provided."
  [db-spec & {:keys [catalog schema table scope nullable? row-fn identifiers as-arrays?]
              :or   {scope :temporary
                     nullable? true
                     row-fn identity
                     identifiers identity}}]
  (jdbc/with-db-metadata [db-meta db-spec]
    (-> db-meta
        (.getBestRowIdentifier catalog schema table (get scopes scope) true)
        (jdbc/metadata-result :row-fn row-fn
                              :identifiers identifiers
                              :as-arrays? as-arrays?))))
