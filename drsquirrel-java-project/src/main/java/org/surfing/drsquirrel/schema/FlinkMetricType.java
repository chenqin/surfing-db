package org.surfing.drsquirrel.schema;

/** These metrics types are defined based on metrics.scope declaration in flink configuraiton. */
public enum FlinkMetricType {
  JOBMANAGER,
  JOBMANAGER_JOB,
  JOBMANAGER_WITH_APPID,
  JOBMANAGER_JOB_WITH_APPID,
  OPERATOR,
  TASK,
  TASKMANAGER,
  TASKMANAGER_JOB,
  TASKMANAGER_JOB_LATENCY,
  UNKNOWN
}
