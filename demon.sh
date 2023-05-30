#!/bin/bash
PROCESS=$1
PIDS=`ps cax | grep $PROCESS | grep -o '^[ ]*[0-9]*'`
if [ -z "$PIDS" ]; then
  echo "Process not running." 1>&2
  cd ~/matcha/build
  nohup GLOG_log_dir=~ mpirun --hostfile ~/matcha/hostfile ~/matcha/build/$PROCESS ~/matcha/surfing-db-java.jar test4 --mca oob_tcp_port_min_v4 7337 -mca btl_tcp_if_exclude lo,docker0 -mca orte_base_help_aggregate 0 &
  exit 1
else
  for PID in $PIDS; do
    echo $PID
  done
fi
