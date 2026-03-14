(ns jepsen.swytch.cert
  "Generates a test-scoped CA and per-node TLS certificates for Swytch
  cluster mTLS. Each test run gets a fresh CA so nodes from different
  runs cannot cross-talk."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]]
            [jepsen.control.util :as cu]))

(def cert-dir "/etc/swytch/certs")

(defn gen-ca!
  "Generates a self-signed CA key + cert on the current node.
  Returns the paths {:ca-key, :ca-cert}."
  []
  (c/su
    (c/exec :mkdir :-p cert-dir)
    (c/exec :openssl "req" "-x509" "-newkey" "ec"
            "-pkeyopt" "ec_paramgen_curve:prime256v1"
            "-keyout" (str cert-dir "/ca-key.pem")
            "-out"    (str cert-dir "/ca.pem")
            "-days"   "1"
            "-nodes"
            "-subj"   "/CN=jepsen-swytch-ca")
    {:ca-key  (str cert-dir "/ca-key.pem")
     :ca-cert (str cert-dir "/ca.pem")}))

(defn gen-node-cert!
  "Generates a node certificate signed by the CA. Expects the CA key
  and cert to already exist at cert-dir on this node. `node` is the
  hostname string."
  [node]
  (let [key-path  (str cert-dir "/" node "-key.pem")
        csr-path  (str cert-dir "/" node ".csr")
        cert-path (str cert-dir "/" node "-cert.pem")
        ext-path  (str cert-dir "/" node "-ext.cnf")]
    (c/su
      ;; Write a SAN extension file so the cert is valid for the hostname
      (c/exec :bash :-c
              (str "cat > " ext-path " <<'EXTEOF'\n"
                   "subjectAltName=DNS:" node ",IP:127.0.0.1\n"
                   "EXTEOF"))
      ;; Generate key + CSR
      (c/exec :openssl "req" "-newkey" "ec"
              "-pkeyopt" "ec_paramgen_curve:prime256v1"
              "-keyout" key-path
              "-out"    csr-path
              "-nodes"
              "-subj"   (str "/CN=" node))
      ;; Sign with CA
      (c/exec :openssl "x509" "-req"
              "-in"      csr-path
              "-CA"      (str cert-dir "/ca.pem")
              "-CAkey"   (str cert-dir "/ca-key.pem")
              "-CAcreateserial"
              "-out"     cert-path
              "-days"    "1"
              "-extfile" ext-path)
      ;; Clean up temporaries
      (c/exec :rm :-f csr-path ext-path))
    {:key  key-path
     :cert cert-path}))

(defn install-ca-from!
  "Copies the CA cert and key from the given source content (strings)
  onto this node."
  [ca-cert-pem ca-key-pem]
  (c/su
    (c/exec :mkdir :-p cert-dir)
    (c/exec :bash :-c (str "cat > " cert-dir "/ca.pem <<'CAPEM'\n" ca-cert-pem "\nCAPEM"))
    (c/exec :bash :-c (str "cat > " cert-dir "/ca-key.pem <<'KEYPEM'\n" ca-key-pem "\nKEYPEM"))))

(defn read-file
  "Reads a remote file's contents as a string."
  [path]
  (c/exec :cat path))
