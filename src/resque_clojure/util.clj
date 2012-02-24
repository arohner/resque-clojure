(ns resque-clojure.util)

(defn includes? [coll e]
  (some #{e} coll))

(defn filter-map [map keys]
  "return a new map containing only the key/value pairs that match the keys"
  (into {} (filter (fn [[k v]] (includes? keys k)) map)))

(defmacro desugar
  "Takes a single s-expr, like (foo bar), evaluates the args, returns a vector of strings"
  [expr]
  `(apply vector ~(var-name (resolve (first expr))) (map str [~@(rest expr)])))