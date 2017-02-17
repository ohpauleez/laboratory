(ns laboratory.experiment
  (:require [clojure.spec]))

(defrecord Experiment [enabled publish metrics use try spec opts])
(defrecord ExperimentSideResult [value metrics])
(defrecord ExperimentResult [name experiment args control candidate spec])

(def always-enabled (constantly true))
(def publish-nowhere (constantly nil))

(defn nanos->ms [^long nanos]
  (/ nanos 1000000.0))

(defn spec-check
  "Checks v against spec; Returns nil if v is valid, or `explain-data` if invalid
  Optionally can:
  :throw-label - Throw an exception when the value isn't valid; Tagged with string label"
  ([spec v]
   (spec-check spec v {}))
  ([spec v opts]
   (when-not (clojure.spec/valid? spec v)
     (let [ed (clojure.spec/explain-data spec v)]
       (if-let [throw-label (:throw-label opts)]
         (throw (ex-info (str "Spec assertion failed for [" throw-label "]! -- "
                              (with-out-str (clojure.spec/explain-out ed)))
                         ed))
         ed)))))

(defn run-with-result
  ([f metrics]
   (let [metrics0 (mapv #(%) (vals metrics))
         t0 (System/nanoTime)
         result (try (f) (catch Exception e e))
         t1 (System/nanoTime)
         metrics1 (mapv #(%) (vals metrics))]
     (->ExperimentSideResult result (merge {:duration-ns (unchecked-subtract t1 t0)}
                                           (zipmap (keys metrics) (map - metrics1 metrics0))))))
  ([f metrics arg1]
   (let [metrics0 (mapv #(%) (vals metrics))
         t0 (System/nanoTime)
         result (try (f arg1) (catch Exception e e))
         t1 (System/nanoTime)
         metrics1 (mapv #(%) (vals metrics))]
     (->ExperimentSideResult result (merge {:duration-ns (unchecked-subtract t1 t0)}
                                           (zipmap (keys metrics) (map - metrics1 metrics0))))))
  ([f metrics arg1 arg2]
   (let [metrics0 (mapv #(%) (vals metrics))
         t0 (System/nanoTime)
         result (try (f arg1 arg2) (catch Exception e e))
         t1 (System/nanoTime)
         metrics1 (mapv #(%) (vals metrics))]
     (->ExperimentSideResult result (merge {:duration-ns (unchecked-subtract t1 t0)}
                                           (zipmap (keys metrics) (map - metrics1 metrics0))))))
  ([f metrics arg1 arg2 arg3]
   (let [metrics0 (mapv #(%) (vals metrics))
         t0 (System/nanoTime)
         result (try (f arg1 arg2 arg3) (catch Exception e e))
         t1 (System/nanoTime)
         metrics1 (mapv #(%) (vals metrics))]
     (->ExperimentSideResult result (merge {:duration-ns (unchecked-subtract t1 t0)}
                                           (zipmap (keys metrics) (map - metrics1 metrics0))))))

  ([f metrics arg1 arg2 arg3 arg4]
   (let [metrics0 (mapv #(%) (vals metrics))
         t0 (System/nanoTime)
         result (try (f arg1 arg2 arg3 arg4) (catch Exception e e))
         t1 (System/nanoTime)
         metrics1 (mapv #(%) (vals metrics))]
     (->ExperimentSideResult result (merge {:duration-ns (unchecked-subtract t1 t0)}
                                           (zipmap (keys metrics) (map - metrics1 metrics0))))))
  ([f metrics arg1 arg2 arg3 arg4 & args]
   (let [metrics0 (mapv #(%) (vals metrics))
         t0 (System/nanoTime)
         result (try (apply f arg1 arg2 arg3 arg4 args) (catch Exception e e))
         t1 (System/nanoTime)
         metrics1 (mapv #(%) (vals metrics))]
     (->ExperimentSideResult result (merge {:duration-ns (unchecked-subtract t1 t0)}
                                           (zipmap (keys metrics) (map - metrics1 metrics0)))))))

(defn make-result
  ([experiment args control-result candidate-result]
   (make-result experiment args control-result candidate-result nil))
  ([experiment args control-result candidate-result spec-result]
   (->ExperimentResult (:name experiment) experiment args control-result candidate-result spec-result)))

(defn run
  "Given an experiment map
  Run the experiment, capturing the `:duration-ns` and other experiment `:metrics`.
  If the results aren't `:publish`ed, the experiment isn't run,
   if no one is looking at the results, the experiment isn't worth conducting."
  ([experiment]
   (if (and ((:enabled experiment always-enabled))
            (:publish experiment))
     (let [control-result (run-with-result (:use experiment) (:metrics experiment))
           candidate-result (run-with-result (:try experiment) (:metrics experiment))
           candidate-spec (when-let [s (:spec experiment)]
                            (spec-check s (:value candidate-result) (:opts experiment {})))]
       ((:publish experiment publish-nowhere) (make-result experiment [] control-result candidate-result candidate-spec))
       (if (instance? Throwable (:value control-result))
         (throw (:value control-result))
         (:value control-result)))
     ((:use experiment))))

  ([experiment arg1]
   (if (and ((:enabled experiment always-enabled) arg1)
            (:publish experiment))
     (let [control-result (run-with-result (:use experiment) (:metrics experiment) arg1)
           candidate-result (run-with-result (:try experiment) (:metrics experiment) arg1)
           candidate-spec (when-let [s (:spec experiment)]
                            (spec-check s (:value candidate-result) (:opts experiment {})))]
       ((:publish experiment publish-nowhere) (make-result experiment [arg1] control-result candidate-result candidate-spec))
       (if (instance? Throwable (:value control-result))
         (throw (:value control-result))
         (:value control-result)))
     ((:use experiment) arg1)))

  ([experiment arg1 arg2]
   (if (and ((:enabled experiment always-enabled) arg1 arg2)
            (:publish experiment))
     (let [control-result (run-with-result (:use experiment) (:metrics experiment) arg1 arg2)
           candidate-result (run-with-result (:try experiment) (:metrics experiment) arg1 arg2)
           candidate-spec (when-let [s (:spec experiment)]
                            (spec-check s (:value candidate-result) (:opts experiment {})))]
       ((:publish experiment publish-nowhere) (make-result experiment [arg1 arg2] control-result candidate-result candidate-spec))
       (if (instance? Throwable (:value control-result))
         (throw (:value control-result))
         (:value control-result)))
     ((:use experiment) arg1 arg2)))


  ([experiment arg1 arg2 arg3]
   (if (and ((:enabled experiment always-enabled) arg1 arg2 arg3)
            (:publish experiment))
     (let [control-result (run-with-result (:use experiment) (:metrics experiment) arg1 arg2 arg3)
           candidate-result (run-with-result (:try experiment) (:metrics experiment) arg1 arg2 arg3)
           candidate-spec (when-let [s (:spec experiment)]
                            (spec-check s (:value candidate-result) (:opts experiment {})))]
       ((:publish experiment publish-nowhere) (make-result experiment [arg1 arg2 arg3] control-result candidate-result candidate-spec))
       (if (instance? Throwable (:value control-result))
         (throw (:value control-result))
         (:value control-result)))
     ((:use experiment) arg1 arg2 arg3)))

  ([experiment arg1 arg2 arg3 arg4]
   (if (and ((:enabled experiment always-enabled) arg1 arg2 arg3 arg4)
            (:publish experiment))
     (let [control-result (run-with-result (:use experiment) (:metrics experiment) arg1 arg2 arg3 arg4)
           candidate-result (run-with-result (:try experiment) (:metrics experiment) arg1 arg2 arg3 arg4)
           candidate-spec (when-let [s (:spec experiment)]
                            (spec-check s (:value candidate-result) (:opts experiment {})))]
       ((:publish experiment publish-nowhere) (make-result experiment [arg1 arg2 arg3 arg4] control-result candidate-result candidate-spec))
       (if (instance? Throwable (:value control-result))
         (throw (:value control-result))
         (:value control-result)))
     ((:use experiment) arg1 arg2 arg3 arg4)))

  ([experiment arg1 arg2 arg3 arg4 & args]
   (if (and (apply (:enabled experiment always-enabled) arg1 arg2 arg3 arg4 args)
            (:publish experiment))
     (let [control-result (apply run-with-result (:use experiment) (:metrics experiment) arg1 arg2 arg3 arg4 args)
           candidate-result (apply run-with-result (:try experiment) (:metrics experiment) arg1 arg2 arg3 arg4 args)
           candidate-spec (when-let [s (:spec experiment)]
                            (spec-check s (:value candidate-result) (:opts experiment {})))]
       ((:publish experiment publish-nowhere) (make-result experiment (into [arg1 arg2 arg3 arg4] args) control-result candidate-result candidate-spec))
       (if (instance? Throwable (:value control-result))
         (throw (:value control-result))
         (:value control-result)))
     (apply (:use experiment) arg1 arg2 arg3 arg4 args))))


(comment

  (require 'clojure.spec)
  (clojure.spec/def ::adder-result number?)

  (defn add5 [x]
    (+ x 5))

  (defn add5fast [^long x]
    ;"5" ;; Use this return to play with the spec checking
    (unchecked-add x 5))

  (def experiment {:name "Add5"
                   :use add5
                   :try add5fast
                   :spec ::adder-result
                   ;:opts {:throw-label "Add5"} ;; These get forwarded `spec-check`
                   :publish prn
                   :metrics {:used-bytes #(- (.totalMemory (Runtime/getRuntime))
                                             (.freeMemory (Runtime/getRuntime)))}})

  (time (add5fast 1))
  (time (run experiment 1))
  )
