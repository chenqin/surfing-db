# FlinkJobWatcher

GLOG_log_dir=~ mpirun -np 18 FlinkJobWatcher ../surfingdb-java.jar testgroupid

# MABS
mpirun --hostfile ~/matcha/hostfile ~/matcha/build/MABS ~/matcha/surfing-db-java.jar test4 --mca oob_tcp_port_min_v4 7337 -mca btl_tcp_if_exclude lo,docker0 -mca orte_base_help_aggregate 0

# contab check
1 * * * * ~/matcha/demon.sh MABS

created @Maui, Hawaii, U.S.A since 2021

# Build & Test (Scripts)

- Build + install deps + build project:
  - `./scripts/build_install.sh`
  - Options:
    - `--no-sudo` to avoid sudo for apt
    - `--arrow-prefix <DIR>` set Arrow install prefix (default: `$HOME/arrow-12-install`)
    - `--arrow-version <X.Y.Z>` set Arrow version (default: `12.0.0`)
  - Options:
    - `--no-sudo` to avoid sudo for apt
    - `--arrow-prefix <DIR>` to set Arrow install prefix (default: `$HOME/arrow-12-install`)

- Run tests:
  - `./scripts/run_tests.sh`
  - Runs C++ unit tests, MPI shuffle tests (np=2,4), and Java tests.
  - For randomized MPI tests you can set `SHUFFLE_TEST_SEED=<uint64>` to make runs deterministic.

Note: Scripts assume Ubuntu with `apt` available. Arrow C++ 12 is built from source.
