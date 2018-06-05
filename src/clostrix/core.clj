(ns clostrix.core
  (:import [rx Observable]
           [com.netflix.hystrix
            HystrixCommand
            HystrixCommand$Setter
            HystrixCommandKey$Factory
            HystrixCommandGroupKey$Factory
            HystrixThreadPoolKey$Factory
            HystrixCommandProperties
            HystrixCommandProperties$Setter]))

(defn command-properties [{:keys [execution-isolation-strategy
                                  ^Integer execution-timeout-in-milliseconds
                                  execution-timeout-enabled
                                  execution-isolation-thread-interrupt-on-timeout
                                  execution-isolation-thread-interrupt-on-cancel
                                  execution-isolation-semaphore-max-concurrent-requests
                                  fallback-isolation-semaphore-max-concurrent-requests
                                  fallback-enabled
                                  circuit-breaker-enabled
                                  circuit-breaker-request-volume-threshold
                                  circuit-breaker-sleep-window-in-milliseconds
                                  circuit-breaker-error-threshold-percentage
                                  circuit-breaker-force-open
                                  circuit-breaker-force-closed
                                  metrics-rolling-statistical-window-in-milliseconds
                                  metrics-rolling-statistical-window-buckets
                                  metrics-rolling-percentile-enabled
                                  metrics-rolling-percentile-window-in-milliseconds
                                  metrics-rolling-percentile-window-buckets
                                  metrics-rolling-percentile-bucket-size
                                  metrics-health-snapshot-interval-in-milliseconds
                                  request-cache-enabled
                                  request-log-enabled]}]
  (let [^HystrixCommandProperties$Setter cs (HystrixCommandProperties/Setter)]
    (when-not (nil? execution-isolation-strategy)
      (.withExecutionIsolationStrategy cs execution-isolation-strategy))
    (when-not (nil? execution-timeout-in-milliseconds)
      (.withExecutionTimeoutInMilliseconds cs execution-timeout-in-milliseconds))
    (when-not (nil? execution-timeout-enabled)
      (.withExecutionTimeoutEnabled cs execution-timeout-enabled))
    (when-not (nil? execution-isolation-thread-interrupt-on-timeout)
      (.withExecutionIsolationThreadInterruptOnTimeout cs execution-isolation-thread-interrupt-on-timeout))
    (when-not (nil? execution-isolation-thread-interrupt-on-cancel)
      (.withExecutionIsolationThreadInterruptOnCancel cs execution-isolation-thread-interrupt-on-cancel))
    (when-not (nil? execution-isolation-semaphore-max-concurrent-requests)
      (.withExecutionIsolationSemaphoreMaxConcurrentRequests cs execution-isolation-semaphore-max-concurrent-requests))
    (when-not (nil? fallback-isolation-semaphore-max-concurrent-requests)
      (.withFallbackIsolationSemaphoreMaxConcurrentRequests cs fallback-isolation-semaphore-max-concurrent-requests))
    (when-not (nil? fallback-enabled)
      (.withFallbackEnabled cs fallback-enabled))
    (when-not (nil? circuit-breaker-enabled)
      (.withCircuitBreakerEnabled cs circuit-breaker-enabled))
    (when-not (nil? circuit-breaker-request-volume-threshold)
      (.withCircuitBreakerRequestVolumeThreshold cs circuit-breaker-request-volume-threshold))
    (when-not (nil? circuit-breaker-sleep-window-in-milliseconds)
      (.withCircuitBreakerSleepWindowInMilliseconds cs circuit-breaker-sleep-window-in-milliseconds))
    (when-not (nil? circuit-breaker-error-threshold-percentage)
      (.withCircuitBreakerErrorThresholdPercentage cs circuit-breaker-error-threshold-percentage))
    (when-not (nil? circuit-breaker-force-open)
      (.withCircuitBreakerForceOpen cs circuit-breaker-force-open))
    (when-not (nil? circuit-breaker-force-closed)
      (.withCircuitBreakerForceClosed cs circuit-breaker-force-closed))
    (when-not (nil? metrics-rolling-statistical-window-in-milliseconds)
      (.withMetricsRollingStatisticalWindowInMilliseconds cs metrics-rolling-statistical-window-in-milliseconds))
    (when-not (nil? metrics-rolling-statistical-window-buckets)
      (.withMetricsRollingStatisticalWindowBuckets cs metrics-rolling-statistical-window-buckets))
    (when-not (nil? metrics-rolling-percentile-enabled)
      (.withMetricsRollingPercentileEnabled cs metrics-rolling-percentile-enabled))
    (when-not (nil? metrics-rolling-percentile-window-in-milliseconds)
      (.withMetricsRollingPercentileWindowInMilliseconds cs metrics-rolling-percentile-window-in-milliseconds))
    (when-not (nil? metrics-rolling-percentile-window-buckets)
      (.withMetricsRollingPercentileWindowBuckets cs metrics-rolling-percentile-window-buckets))
    (when-not (nil? metrics-rolling-percentile-bucket-size)
      (.withMetricsRollingPercentileBucketSize cs metrics-rolling-percentile-bucket-size))
    (when-not (nil? metrics-health-snapshot-interval-in-milliseconds)
      (.withMetricsHealthSnapshotIntervalInMilliseconds cs metrics-health-snapshot-interval-in-milliseconds))
    (when-not (nil? request-cache-enabled)
      (.withRequestCacheEnabled cs request-cache-enabled))
    (when-not (nil? request-log-enabled)
      (.withRequestLogEnabled cs request-log-enabled))
    cs))

(defn command-setter [default-group-key {:keys [^String command-key
                                                ^String thread-pool-key
                                                properties]
                                         :as opts}]
  (let [^String group-key (or (:group-key opts) default-group-key)
        ^HystrixCommandGroupKey gk (HystrixCommandGroupKey$Factory/asKey group-key)
        ^HystrixCommand$Setter setter (HystrixCommand$Setter/withGroupKey gk)]
    (when-not (nil? command-key)
      (let [^HystrixCommandKey ck (HystrixCommandKey$Factory/asKey command-key)]
        (.andCommandKey setter ck)))
    (when-not (nil? thread-pool-key)
      (let [^HystrixThreadPoolKey tk (HystrixThreadPoolKey$Factory/asKey thread-pool-key)]
        (.andThreadPoolKey setter tk)))
    (when-not (nil? properties)
      (let [^HystrixCommandProperties p (command-properties properties)]
        (.andCommandPropertiesDefaults setter p)))
    setter))

(defn command
  ([group-key f]
   (command group-key f {}))
  ([group-key f {:keys [fallback] :as opts}]
   (proxy [HystrixCommand] [^HystrixCommand$Setter (command-setter group-key opts)]
     (run []
       (f))
     (getFallback []
       (if fallback
         (fallback)
         (throw (UnsupportedOperationException. "No :fallback-fn provided")))))))

(defn execute [^HystrixCommand command]
  (.execute command))

(defn queue [^HystrixCommand command]
  (.queue command))

(defmacro with-command [opts & body]
  `(execute (command (str (ns-name *ns*))
                     #(do ~@body)
                     ~opts)))
