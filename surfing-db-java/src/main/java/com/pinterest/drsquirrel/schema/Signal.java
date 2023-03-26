package com.pinterest.drsquirrel.schema;

public interface Signal {

  enum SignalType {
    JOB_LOG,
    FLINK_METRIC
  }

  SignalType getSignalType();

  // Signal must at least only one of both.
  String getApplicationId();

  String getJobId();

  String getJobName();

  String getCluster();

  long getTimestamp();

  default String getSaltKey() {
    throw new UnsupportedOperationException("not supported");
  }
}
