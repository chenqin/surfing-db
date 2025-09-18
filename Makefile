SHELL := /bin/bash

.PHONY: help build test test-no-mpi nested-mpi java-jar thrift-bench load-all-cores shuffle-all-cores cogroup-all-cores example

help:
	@echo "Common targets:"
	@echo "  make build              # Install deps (APT Arrow by default) + build"
	@echo "  make test               # Run C++ unit, MPI (if enabled), and Java tests"
	@echo "  make test-no-mpi        # Run tests but skip MPI"
	@echo "  make nested-mpi         # Run nested MPI tests (np=2,4)"
	@echo "  make java-jar           # Build shaded Java jar (drsquirrel-java)"
	@echo "  make thrift-bench       # Run JNI Thrift decode benchmarks"
	@echo "  make shuffle-all-cores  # Run JNI shuffle load across all cores"
	@echo "  make cogroup-all-cores  # Run JNI cogroup load across all cores"
	@echo "  make load-all-cores     # Run both shuffle + cogroup all-cores"
	@echo "  make example            # Build + run fake cogroup Java example (np=4)"

# One-shot build (Arrow via APT by default). Pass CMAKE_EXTRA_FLAGS as needed.
build:
	./scripts/build_install.sh

test:
	./scripts/run_tests.sh

test-no-mpi:
	./scripts/run_tests.sh --no-mpi

nested-mpi:
	./scripts/run_nested_mpi_tests.sh --np all

java-jar:
	mvn -q -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package

thrift-bench:
	./scripts/run_thrift_arrow_bench.sh

shuffle-all-cores:
	./scripts/run_shuffle_all_cores.sh

cogroup-all-cores:
	./scripts/run_cogroup_all_cores.sh

load-all-cores:
	./scripts/run_all_load_all_cores.sh

example:
	bash scripts/run_fake_cogroup_example.sh --build --np 4 --mode one --rows 100000 --iters 1

