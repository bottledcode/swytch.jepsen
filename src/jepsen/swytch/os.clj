(ns jepsen.swytch.os
  "Custom OS setup for Hetzner servers. Wraps jepsen.os.debian but
  forces an apt-get update before installing packages, since Hetzner
  minimal images often have stale or incomplete package caches."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [os :as os]]
            [jepsen.os.debian :as debian]))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian (with forced apt update)")
      (debian/setup-hostfile!)
      (c/su (c/exec :apt-get :update))
      (debian/install [:apt-transport-https
                       :apt-utils
                       :build-essential
                       :bzip2
                       :curl
                       :dirmngr
                       :faketime
                       :iproute2
                       :iptables
                       :iputils-ping
                       :logrotate
                       :man-db
                       :ntpsec-ntpdate
                       :psmisc
                       :rsyslog
                       :unzip
                       :wget])
      (c/su
        (c/exec :update-alternatives :--set :iptables "/usr/sbin/iptables-legacy")
        (c/exec :update-alternatives :--set :ip6tables "/usr/sbin/ip6tables-legacy")))

    (teardown! [_ test node])))
