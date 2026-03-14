(defproject jepsen.swytch "0.1.0-SNAPSHOT"
  :description "Jepsen test suite for Swytch distributed cache"
  :url "https://github.com/bottled-codes/swytch.jepsen"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.swytch
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11-SNAPSHOT"]
                 [io.jepsen/generator "0.1.1"]
                 [jepsen.txn "0.1.3"]
                 [elle "0.2.6"]
                 [com.taoensso/carmine "3.5.0"]]
  :jvm-opts ["-Xmx8g"
             "-Djava.awt.headless=true"
             "-server"]
  :profiles {:uberjar {:aot :all}})
