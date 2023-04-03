package com.pinterest.drsquirrel.schema;

import io.airlift.units.Duration;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** JobState aggregates all metrics from JM */
public class JobState {
  private static final int MAX_CHECKPOINT_PATHS_COUNT = 3;
  // basic job stats
  private Duration jobUptime;
  private int jobFullRestarts = 0;

  // checkpoint stats
  private int failedCheckpoints = 0;
  private long checkpointSize;
  private Duration checkpointDurationInMs;
  private Map<Long, String> checkpointPaths = new TreeMap<>();

  public JobState() {}

  public JobBasicStats getJobBasicStats() {
    return new JobBasicStats(jobUptime, jobFullRestarts);
  }

  public CheckpointStats getCheckpointStats() {
    return new CheckpointStats(
        failedCheckpoints, checkpointSize, checkpointDurationInMs, checkpointPaths);
  }

  public void update(FlinkMetric metric) {
    String metricName = metric.getName();
    if (metricName.equals("uptime")) {
      long uptime = Long.parseLong(metric.getValue());
      jobUptime = new Duration(uptime, TimeUnit.MILLISECONDS);
    } else if (metricName.equals("fullRestarts") || metricName.equals("numRestarts")) {
      jobFullRestarts = Integer.parseInt(metric.getValue());
    }
    if (metricName.equals("numberOfFailedCheckpoints")) {
      failedCheckpoints = Integer.parseInt(metric.getValue());
    } else if (metricName.equals("lastCheckpointSize")) {
      checkpointSize = Long.parseLong(metric.getValue());
    } else if (metricName.equals("lastCheckpointDuration")) {
      long duration = Long.parseLong(metric.getValue());
      checkpointDurationInMs = new Duration(duration, TimeUnit.MILLISECONDS);
    } else if (metricName.equals("lastCheckpointExternalPath")) {
      checkpointPaths.put(metric.getTimestamp(), metric.getValue());
      if (checkpointPaths.size() > MAX_CHECKPOINT_PATHS_COUNT) {
        ((TreeMap<Long, String>) checkpointPaths).pollFirstEntry();
      }
    }
  }

  public Duration getJobUptime() {
    return jobUptime;
  }

  public void setJobUptime(Duration jobUptime) {
    this.jobUptime = jobUptime;
  }

  public int getJobFullRestarts() {
    return jobFullRestarts;
  }

  public void setJobFullRestarts(int jobFullRestarts) {
    this.jobFullRestarts = jobFullRestarts;
  }

  public int getFailedCheckpoints() {
    return failedCheckpoints;
  }

  public void setFailedCheckpoints(int failedCheckpoints) {
    this.failedCheckpoints = failedCheckpoints;
  }

  public long getCheckpointSize() {
    return checkpointSize;
  }

  public void setCheckpointSize(long checkpointSize) {
    this.checkpointSize = checkpointSize;
  }

  public Duration getCheckpointDurationInMs() {
    return checkpointDurationInMs;
  }

  public void setCheckpointDurationInMs(Duration checkpointDurationInMs) {
    this.checkpointDurationInMs = checkpointDurationInMs;
  }
}
