(ns resque-clojure.core
  (:use [resque-clojure.util :only [filter-map]])
  (:require [resque-clojure.resque :as resque]
            [resque-clojure.redis :as redis]
            [resque-clojure.supervisor :as supervisor]))


(defn configure [c]
  "set configuration parameters. Available keys:
     :host
     :port
     :timeout
     :password
     :max-shutdown-wait
     :poll-interval
     :max-workers"
  (let [redis-keys (keys @redis/config)
        super-keys (keys @supervisor/config)
        redis-map (filter-map c redis-keys)
        super-map (filter-map c super-keys)]
    (redis/configure redis-map)
    (supervisor/configure super-map)))

(defn var-name [v]
  (str (-> v (meta) :ns) "/" (-> v (meta) :name)))

(defmacro desugar
  "Takes a single s-expr, like (foo bar), evaluates the args, returns a vector of strings"
  [expr]
  `(apply vector ~(var-name (resolve (first expr))) (map str [~@(rest expr)]) ))

(defn enqueue* [queue worker-name & args]
  "create a new resque job
     queue: name of the queue (does not start with resque:queue)
     worker-name: fully-qualified function name (ex. clojure.core/str in clojure, MyWorker in ruby)
     args: data to be sent as args to the worker. must be able to be serialized to json"
  (apply resque/enqueue queue worker-name args))

(defmacro enqueue [queue expr]
  `(let [[worker-name# & args#] (desugar ~expr)]
     (apply enqueue* ~queue worker-name# args#)))

(defn start [queues]
  "start listening for jobs on queues (vector)."
  (supervisor/start queues))

(defn stop []
  "stops polling queues. waits for all workers to complete current job"
  (supervisor/stop))
