(ns jepsen.swytch.db-test
  "Smoke test: verifies a 5-node Swytch cluster starts and accepts
  basic Redis GET/SET commands through Jepsen."
  (:require [clojure.test :refer :all]
            [jepsen [core :as jepsen]
                    [tests :as tests]]
            [jepsen.swytch :as swytch]))

(deftest ^:integration smoke-test
  (testing "5-node cluster starts and accepts Redis commands"
    (let [test (jepsen/run!
                 (swytch/swytch-test
                   {:nodes      ["n1" "n2" "n3" "n4" "n5"]
                    :ssh        {:username "root"
                                 :strict-host-key-checking false}
                    :time-limit 30}))]
      (is (:valid? (:results test))))))
