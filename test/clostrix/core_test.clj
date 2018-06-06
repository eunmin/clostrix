(ns clostrix.core-test
  (:require [clojure.test :refer :all]
            [clostrix.core :refer :all])
  (:import [com.netflix.hystrix HystrixCommand]
           [com.netflix.hystrix.exception HystrixRuntimeException]))

(deftest command-test
  (testing "Set CommandGroupKey"
    (let [^HystrixCommand cmd (command "test-group-key" (constantly 1))]
      (is (= "test-group-key" (.name (.getCommandGroup cmd))))))

  (testing "Synchronous Execution"
    (is (= 1 (execute (command "test-group-key" (constantly 1))))))

  (testing "Asynchronous Execution"
    (is (= 1 @(queue (command "test-group-key" (constantly 1))))))

  (testing "Set CommandKey"
    (let [^HystrixCommand cmd (command "test-group-key"
                                       (constantly 1)
                                       {:command-key "test-command-key"})]
      (is (= "test-command-key" (.name (.getCommandKey cmd))))))

  (testing "Set ThreadPoolKey"
    (let [^HystrixCommand cmd (command "test-group-key"
                                       (constantly 1)
                                       {:thread-pool-key "test-thread-pool-key"})]
      (is (= "test-thread-pool-key" (.name (.getThreadPoolKey cmd))))))

  (testing "Set CommandProperties"
    (let [^HystrixCommand cmd (command "test-group-key2"
                                       (constantly 1)
                                       {:command-key "test-command-properties-key"
                                        :properties {:execution-timeout-in-milliseconds 3000}})]
      (is (= 3000 (.get (.executionTimeoutInMilliseconds (.getProperties cmd)))))))

  (testing "Fallback"
    (let [^HystrixCommand cmd (command "test-group-key"
                                       #(throw (ex-info "Fallback test" {}))
                                       {:fallback (constantly 0)})]
      (is (zero? (execute cmd)))))

  (testing "Cache"
    (let [ctx (initialize-context)
          ^HystrixCommand cmd (command "test-group-key"
                                       (constantly 1)
                                       {:cache-key "1"})]
      (is (= 1 (execute cmd)))
      (shutdown-context ctx)))

  (testing "with-command"
    (is (= 2 (with-command {:group-key "test-group-key"
                            :command-key "test-command-key"}
               (inc 1)))))

  (testing "with-request-context"
    (with-request-context
      (let [^HystrixCommand cmd (command "test-group-key"
                                         (constantly 1)
                                         {:cache-key "1"})]
        (is (= 1 (execute cmd)))))))
