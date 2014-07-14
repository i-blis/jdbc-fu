(ns jdbc-fu.datatypes
  (:import java.lang.reflect.Modifier))

(defn- static-fields
  "Given a java class, returns a hash-map which maps the class' static field names
   (as string) to their values. Providing a `:reverse` optional argument  turns
   the result in a map of field values to field names."
  [class & options]
  (let [arrange (if (some #(= :reverse %) options) (comp vec reverse) identity)]
    (->> (.getDeclaredFields class)
         (filter #(let [m (.getModifiers %)]
                    (and (Modifier/isStatic m)
                         (Modifier/isPublic m))))
         (map #(arrange (vector (.getName %) (.get % nil))))
         (into {})
     )))

(def
  ^{:doc "A map of the values of (the statically defined) SQL types to their name"}
  sql-types
  (static-fields java.sql.Types :reverse))
