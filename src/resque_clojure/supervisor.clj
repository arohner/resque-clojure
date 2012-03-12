(ns resque-clojure.supervisor
  (:require [resque-clojure.worker :as worker]
            [resque-clojure.resque :as resque]))

(def run-loop? (ref true))
(def working-agents (ref #{}))
(def idle-agents (ref #{}))

(def watched-queues (ref {}))

(def config (atom {:max-shutdown-wait (* 10 1000) ;; milliseconds
                   :poll-interval (* 5 1000)}))

(declare release-worker reserve-worker make-agent listen-to listen-loop dispatch-jobs)

(defn configure [c]
  (swap! config merge c))

(defn get-queues [agent]
  (get @watched-queues agent))

(defn set-queues [agent queues]
  (dosync
   (alter watched-queues update-in [agent] (constantly queues))))

(defn start-agents [queues]
  (doseq [[q count] queues]
    (dotimes [i count]
      (make-agent :queues q))))

(defn start [queues]
  "start listening for jobs on queues (vector)."
  (start-agents queues)
  (dosync (ref-set run-loop? true))
  (.start (Thread. listen-loop "Resque Listen Loop")))

(defn stop []
  "stops polling queues. waits for all workers to complete current job"
  (dosync (ref-set run-loop? false))
  (apply await-for (:max-shutdown-wait @config) @working-agents))

(defn worker-complete [key agent old-state new-state]
  (release-worker agent)
  (when @run-loop?
    (dispatch-jobs :queues (get-queues agent)))
  (if (= :error (:result new-state))
    (resque/report-error new-state)))

(defn dispatch-jobs [& {:keys [queues]}]
  (when-let [worker-agent (reserve-worker)]
    (let [msg (resque/dequeue (get-queues worker-agent))]
      (if msg
        (send-off worker-agent worker/work-on msg)
        (release-worker worker-agent)))))

(defn listen-loop []
  (if @run-loop?
    (do
      (dispatch-jobs)
      (Thread/sleep (:poll-interval @config))
      (recur))))

(defn make-agent [& {:keys [queues]}]
  (let [worker-agent (agent {} :error-handler (fn [a e] (throw e)))]
    (set-queues worker-agent queues)
    (add-watch worker-agent :worker-complete worker-complete)
    (dosync (commute idle-agents conj worker-agent))
    worker-agent))

(defn reserve-worker []
  "either returns an idle worker or nil.
   marks the returned worker as working."
  (when-let [w (first @idle-agents)]
    (dosync
     (alter idle-agents disj w)
     (alter working-agents conj w))
    (resque/register (get-queues w))
    w))

(defn release-worker [w]
  (dosync (alter working-agents disj w)
          (alter idle-agents conj w))
  (resque/unregister (get-queues w))
  w)

;; Runtime.getRuntime().addShutdownHook(new Thread() {
;;     public void run() { /*
;;        my shutdown code here
;;     */ }
;;  });

