nodes := "n1.swytch.earth,n2.swytch.earth,n3.swytch.earth,n4.swytch.earth,n5.swytch.earth"
ssh_key := "~/.ssh/id_rsa"
time_limit := "60"

_run workload nemesis *extra_args:
    lein run test --nodes {{nodes}} --ssh-private-key {{ssh_key}} --workload {{workload}} --nemesis-config {{nemesis}} --time-limit {{time_limit}} {{extra_args}}

# Safe nemesis
safe-counter *args: (_run "counter" "safe" args)
safe-set *args: (_run "set" "safe" args)
safe-sorted-set *args: (_run "sorted-set" "safe" args)

# No nemesis
none-counter *args: (_run "counter" "none" args)
none-set *args: (_run "set" "none" args)
none-sorted-set *args: (_run "sorted-set" "none" args)

# Run all workloads with a given nemesis
safe-all: safe-counter safe-set safe-sorted-set
none-all: none-counter none-set none-sorted-set
