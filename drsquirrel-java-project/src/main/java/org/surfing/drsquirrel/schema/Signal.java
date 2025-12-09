package org.surfing.drsquirrel.schema;

public interface Signal {

  SignalType getSignalType();

  // Signal must at least only one of both.
  String getApplicationId();

  String getJobId();

  String getJobName();

  String getCluster();

  long getTimestamp();

  default String getSaltKey() {
    return null;
    //throw new UnsupportedOperationException("not supported");
  }

  enum SignalType {
    JOB_LOG,
    FLINK_METRIC
  }
}
