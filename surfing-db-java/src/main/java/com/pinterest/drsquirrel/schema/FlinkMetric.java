package com.pinterest.drsquirrel.schema;

import java.io.Serializable;

public class FlinkMetric implements Signal, Cloneable, Serializable {
  public static final String JVM_HEAP_COMMITTED = "Status.JVM.Memory.Heap.Committed";
  public static final String JVM_HEAP_USED = "Status.JVM.Memory.Heap.Used";
  public static final String JVM_NONHEAP_COMMITTED = "Status.JVM.Memory.NonHeap.Committed";
  public static final String JVM_NONHEAP_USED = "Status.JVM.Memory.NonHeap.Used";

  public static final String BUFFER_INPOOL_USAGE = "buffers.inPoolUsage";
  public static final String BUFFER_OUTPOOL_USAGE = "buffers.outPoolUsage";

  public static final String RECORDS_IN_RATE = "numRecordsInPerSecond.rate";
  public static final String RECORDS_OUT_RATE = "numRecordsOutPerSecond.rate";

  private FlinkMetricType type;
  private long timestamp;
  private String host;
  private String fullName;
  private String name;
  private String value;
  private String taskManagerId;
  private String jobId;
  private String jobName;
  private String operatorId;
  private String operatorName;
  private int subtaskIndex;
  private String taskId;
  private String taskName;
  private String taskAttemptId;
  private int taskAttemptNum;
  private String cluster;
  private String applicationId;

  public FlinkMetric() {
    timestamp = -1L;
    host = "unknown";
  }

  public FlinkMetricType getType() {
    return this.type;
  }

  public void setType(FlinkMetricType metricType) {
    this.type = metricType;
  }

  public String getTaskManagerId() {
    return taskManagerId;
  }

  public void setTaskManagerId(String tmId) {
    this.taskManagerId = tmId;
  }

  public String getOperatorId() {
    return operatorId;
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }

  public String getOperatorName() {
    return operatorName;
  }

  public void setOperatorName(String operatorName) {
    this.operatorName = operatorName;
  }

  public int getSubtaskIndex() {
    return subtaskIndex;
  }

  public void setSubtaskIndex(int subIndex) {
    this.subtaskIndex = subIndex;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public int getIntValue() {
    return Integer.parseInt(this.value);
  }

  public double getDouble() {
    return Double.parseDouble(this.value);
  }

  public long getLong() {
    return Long.parseLong(this.value);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getFullName() {
    return fullName;
  }

  public void setFullName(String fullName) {
    this.fullName = fullName;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public SignalType getSignalType() {
    return SignalType.FLINK_METRIC;
  }

  @Override
  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  @Override
  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  @Override
  public String toString() {
    return String.format(
        "timestamp: %s, host: %s, metric: %s, value: %s", timestamp, host, fullName, value);
  }

  public String getTaskAttemptId() {
    return taskAttemptId;
  }

  public void setTaskAttemptId(String attemptId) {
    this.taskAttemptId = attemptId;
  }

  public int getTaskAttemptNum() {
    return taskAttemptNum;
  }

  public void setTaskAttemptNum(int attemptNum) {
    this.taskAttemptNum = attemptNum;
  }
}
