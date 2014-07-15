(defproject jdbc-fu "0.1.1"
  :description "A collection of utility functions on top of java.jdbc"
  :url "https://github.com/i-blis/jdbc-fu"
  :license {:name "WTFPL"
            :url "http://www.wtfpl.net/txt/copying/"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.4"]]
  :profiles {:dev {:dependencies [[org.xerial/sqlite-jdbc "3.7.2"]
                                  [expectations "2.0.7"]]}})
