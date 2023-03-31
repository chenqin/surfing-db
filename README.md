# HPCArrow

A efficient and powerful HPC workload engine with MPI and Apache Arrow.

# features

- Unified data communication (ML / Data) under MPI protocol
- Efficient languages support with Apache Arrow 
- XGBoost Pytorch GPU support
# how to run
- run install.sh for dependencies

cd build
mpirun --hostfile hostfile KafkaExample --mca oob_tcp_port_min_v4 7337 -mca btl_tcp_if_exclude lo,docker0

# configuration
- src/meta/schema.h 
-- MAX_STR_LEN defines max size of string before truncate (lower is better for perf)
-- MEM_PAGE_SIZE defines mtable pre allocated memory


created @Maui, Hawaii, U.S.A since 2021
