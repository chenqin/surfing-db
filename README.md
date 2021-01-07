# surfing-db
Ride wave of streams with SQL/Python on HPC architectures (NUMA, GPU, RMA)

The goal of this project is to enable scientist, analyst and who speaks SQL and python take advantage of more efficent cluster architecture on open cloud run experiment and machine learning in sub-second latency.

## unified memory model with arrow
Row schema & ser/desr were managed as arrow buffer and shared_ptr to minimize data copy in/out of surfing-db

After row land to columnar table, it is saved as columnar format to maximize query speed.

## unified computation architecture with MPI
Unlike other big data engines, surfing-db treats data science and machine learning as first class citizen. 
It runs on computational heavy architecture (de facto standard of distributed AI/ML) with OpenMP optimization.
Large scale machine learning workload can plugin and natively run in surfing-db cluster.

## Standard SQL interface
Define and query surfing-db tables with same SQL you are familiar with

`
CREATE TABLE table_name (
 	field_name data_type constrain_name PRIMARY KEY, 
 	field_name data_type constrain_name
 );
 `
 
`
INSERT INTO table_name(field_name...) select xxx from aa where yyy...
`


pre-requesite
- sudo apt install libjemalloc-dev
- install mpich 3.3.2

How to run

- mpirun -np 12 ./MainTest

Created @Maui, Hawaii, U.S.A
