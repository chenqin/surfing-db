# FlinkJobWatcher

GLOG_log_dir=~ mpirun -np 18 FlinkJobWatcher ../surfingdb-java.jar testgroupid

# MABS
mpirun --hostfile ~/matcha/hostfile ~/matcha/build/MABS ~/matcha/surfing-db-java.jar test4 --mca oob_tcp_port_min_v4 7337 -mca btl_tcp_if_exclude lo,docker0 -mca orte_base_help_aggregate 0

# contab check
1 * * * * ~/matcha/demon.sh MABS

created @Maui, Hawaii, U.S.A since 2021
