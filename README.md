# surfing-db

A efficient and powerful in memory kafka messages processor using MPI and Apache Arrow.

# features

- in-memory data processor, O(1) access low level memory layout optimization. row oriented maper and shuffe, columnar oriented slice and dice
- kafka consumer sharding with MPI based distributed programming and zero copy arrow rpce
- user defined partioner decouple ingest workers from rest of workloads (aka map reduce, slice/dice rpc stages)
# how to run
- run install.sh for dependencies
- mpirun --hostfile hostfile ./MainTest --mca oob_tcp_port_min_v4 <port> -mca btl_tcp_if_exclude lo,docker0

# configuration
- src/meta/schema.h 
-- MAX_STR_LEN defines max size of string before truncate (lower is better for perf)
-- MEM_PAGE_SIZE defines mtable pre allocated memory

created @Maui, Hawaii, U.S.A since 2021
