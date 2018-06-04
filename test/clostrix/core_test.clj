(ns clostrix.core-test
  (:require [clojure.test :refer :all]
            [clostrix.core :refer :all])
  (:import [com.netflix.hystrix HystrixCommand]))

(deftest command-test
  (testing "Set CommandGroupKey"
    (let [^HystrixCommand cmd (command "test-group-key" (constantly 1))]
      (is (= "test-group-key" (.name (.getCommandGroup cmd))))))

  (testing "Synchronous Execution"
    (is (= 1 (execute (command "test-group-key" (constantly 1))))))

  (testing "Asynchronous Execution"
    (is (= 1 @(queue (command "test-group-key" (constantly 1))))))

  (testing "Set CommandKey"
    (let [^HystrixCommand cmd (command "test-group-key" (constantly 1)
                                       {:command-key "test-command-key"})]
      (is (= "test-command-key" (.name (.getCommandKey cmd))))))

  (testing "Set ThreadPoolKey"
    (let [^HystrixCommand cmd (command "test-group-key" (constantly 1)
                                       {:thread-pool-key "test-thread-pool-key"})]
      (is (= "test-thread-pool-key" (.name (.getThreadPoolKey cmd))))))

  (testing "Set CommandProperties"
    (let [^HystrixCommand cmd (command "test-group-key2" (constantly 1)
                                       {:command-key "test-command-properties-key"
                                        :properties {:execution-timeout-in-milliseconds 3000}})]
      (println "2:" (.name (.getCommandKey cmd)))
      (is (= 3000 (.get (.executionTimeoutInMilliseconds (.getProperties cmd)))))))

  (testing "defcommand"
    (defcommand test-command
      {}
      [x]
      x)
    (is (= 1 (test-command 1)))))
