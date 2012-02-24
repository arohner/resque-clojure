(ns resque-clojure.test.scheduler
  (:use [clojure.test]
        [resque-clojure.scheduler])
  (:require [clj-time.core :as time]
            [resque-clojure.test.helper :as helper]
            [clojure.data.json :as json]
            [resque-clojure.redis :as redis]
            [resque-clojure.resque :as resque]))

(use-fixtures :once helper/redis-test-instance)
(use-fixtures :each helper/cleanup-redis-keys)

(def test-queue "test-queue")

(deftest test-future-queue-name
  (= "resque:future:queue:test-queue" (future-queue "test-queue")))

(deftest test-enqueue-at
  (let [t (time/plus (time/now) (time/hours 1))]
    (enqueue-at t test-queue (inc 3))
    (let [val (first (redis/zrange (future-queue test-queue) 0 -1))]
      (is (= {:class "clojure.core/inc", :args ["3"]}
             (json/read-json val)))
      (is (= (.getMillis t) (long (redis/zscore (future-queue test-queue) val)))))))

(deftest test-transfer
  (let [t (time/minus (time/now) (time/minutes 1))]
    (enqueue-at t test-queue (inc 3))
    (transfer test-queue)
    (is (= 0 (redis/zcard (future-queue test-queue))))

    (is (= {:class "clojure.core/inc", :args ["3"]}
           (-> test-queue (resque/-full-queue-name) (redis/lpop) (json/read-json))))))

(deftest test-doesnt-transfer-future-jobs
  (let [t (time/plus (time/now) (time/hours 1))]
    (enqueue-at t test-queue (inc 3))
    (transfer test-queue)
    (is (= 1 (redis/zcard (future-queue test-queue))))
    (is (= 0 (redis/llen (resque/-full-queue-name test-queue))))))

(deftest test-transfer-all
  (let [t (time/minus (time/now) (time/minutes 1))]
    (enqueue-at t test-queue (inc 3))
    (is (= ["test-queue"] (all-future-queues)))
    (transfer-all)
    (is (= 0 (redis/zcard (future-queue test-queue))))

    (is (= {:class "clojure.core/inc", :args ["3"]}
           (-> test-queue (resque/-full-queue-name) (redis/lpop) (json/read-json))))))