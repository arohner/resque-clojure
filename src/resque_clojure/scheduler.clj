(ns resque-clojure.scheduler
  (:require [resque-clojure.redis :as redis])
  (:require [resque-clojure.resque :as resque])
  (:require [clojure.data.json :as json])
  (:require [clj-time.core :as time])
  (:use [resque-clojure.util :only (desugar)]))

(declare enqueue-at* future-queue transfer transfer-all)

;; public

(defmacro enqueue-at [at queue expr]
  `(let [[worker-name# & args#] (desugar ~expr)]
     (apply enqueue-at* ~at ~queue worker-name# args#)))

(defn start []
  (future
    (loop []
      (transfer-all)
      (Thread/sleep (* 60 1000))
      (recur))))

;; private

(defn future-queue
  [queue-name]
  (resque/-namespace-key (str "future:queue:" queue-name)))

(defn enqueue-at*
  "run a job on/after the specified time. "
  [at queue worker-name & args]
  (redis/zadd (future-queue queue)
              (double (.getMillis at))
              (json/json-str {:class worker-name :args args})))

(defn transfer
  "The resque job that checks if it's time to run any jobs"
  [queue-name]
  (redis/with-connection
    (let [f-queue (future-queue queue-name)
          queue (resque/-full-queue-name queue-name)
          _ (.watch redis/redis (into-array [f-queue]))
          time (.getMillis (time/now))
          jobs (redis/zrange-by-score f-queue (double 0) (double time))
          transaction (.multi redis/redis)]
      (.zremrangeByScore transaction f-queue (double 0) (double time))
      (doseq [j jobs]
        (.rpush transaction queue j))
      (.exec transaction))))

(defn all-future-queues []
  (map #(second (re-find #"^resque:future:queue:(.*)$" %)) (redis/keys (future-queue "*"))))

(defn transfer-all []
  (doseq [q (all-future-queues)]
    (println "transfer:" q)
    (transfer q)))

