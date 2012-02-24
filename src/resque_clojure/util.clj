(ns resque-clojure.util)

(defn includes? [coll e]
  (some #{e} coll))

(defn filter-map [map keys]
  "return a new map containing only the key/value pairs that match the keys"
  (into {} (filter (fn [[k v]] (includes? keys k)) map)))

(defn var-name [v]
  (str (-> v (meta) :ns) "/" (-> v (meta) :name)))

(defmacro desugar
  "Takes a single s-expr, like (foo bar), evaluates the args, returns a vector of strings"
  [expr]
  `(let [v# (var ~(first expr))]
     (apply vector (var-name v#) (map str [~@(rest expr)]))))