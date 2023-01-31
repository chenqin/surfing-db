# surfing-db

A efficient and powerful in memory kafka messages processor using MPI and Apache Arrow.

# features

- in-memory data processor, low level memory layout optimization. row oriented prefilter and shuffe, columnar oriented slice and dice
- kafka consumer sharding with MPI based distributed programming and zero copy arrow rpc

# how to run
- run install.sh for dependencies

# configuration
- src/meta/schema.h 
-- MAX_STR_LEN defines max size of string before truncate (lower is better for perf)
-- MEM_PAGE_SIZE defines mtable pre allocated memory

created @Maui, Hawaii, U.S.A since 2021
