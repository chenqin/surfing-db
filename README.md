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
  - By default installs Apache Arrow C++ from APT (libarrow-dev, libparquet-dev, libarrow-dataset-dev) and builds the project.
  - Options:
    - `--no-sudo` to avoid sudo for apt
    - `--use-system-arrow` (default) to install Arrow via APT
    - `--build-arrow` to build Arrow from source
    - `--arrow-prefix <DIR>` Arrow install prefix for source build (default: `$HOME/arrow-12-install`)
    - `--arrow-version <X.Y.Z>` Arrow version for source build (default: `12.0.0`)
  - Options:
    - `--no-sudo` to avoid sudo for apt
    - `--arrow-prefix <DIR>` to set Arrow install prefix (default: `$HOME/arrow-12-install`)

- Run tests:
  - `./scripts/run_tests.sh`
  - Runs C++ unit tests, MPI shuffle tests (np=2,4), and Java tests.
  - For randomized MPI tests you can set `SHUFFLE_TEST_SEED=<uint64>` to make runs deterministic.

## How To Run Locally (Quick Guide)

Prerequisites
- Ubuntu/Debian with `apt` (for system Arrow install)
- OpenJDK 8 (installed by script)
- OpenMPI (installed by script: `openmpi-bin`, `libopenmpi-dev`)

Build
- Default (uses system Arrow packages):
  - `./scripts/build_install.sh`
- Enable MPI tests at configure time:
  - `CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON ./scripts/build_install.sh`

Run all tests
- Full suite (includes MPI if enabled):
  - `./scripts/run_tests.sh`
- Quick (skip MPI):
  - `./scripts/run_tests.sh --no-mpi`

Run MPI tests manually
- From `build/`:
  - `export SHUFFLE_TEST_SEED=12345`
  - `ctest -R "MpiShuffleTest|MpiTwoSideShuffleTest_np2|MpiTwoSideShuffleTest_np4|MpiShuffleRandom_one_np4|MpiShuffleRandom_two_np4" --output-on-failure`

Run Java tests (JNI included)
- Ensure native libs are discoverable:
  - `export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH`
- Run:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 test`

MPI Java JNI Runner (2 ranks)
- Build shaded jar:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- Run under MPI:
  - `mpiexec -np 2 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniFlinkJobWatcherRunner`


## Kafka JNI test (optional)

The Java JNI test for Kafka will run only if you provide environment variables:

- `KAFKA_SERVERSET`: Path to a text file with one broker per line, e.g.:
  ```
  localhost:9092
  127.0.0.1:9092
  ```
- `KAFKA_TOPICS`: Comma-separated list of topics, e.g. `metrics_topic,logs_topic`
- `KAFKA_GROUP_ID`: A consumer group id, e.g. `flink-watcher-test`

Example:

```
export KAFKA_SERVERSET=$PWD/serverset.txt
export KAFKA_TOPICS=metrics,logs
export KAFKA_GROUP_ID=flink-watcher-test
export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH
mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 test
```

Note: Scripts assume Ubuntu with `apt` available. Arrow C++ 12 is built from source.
