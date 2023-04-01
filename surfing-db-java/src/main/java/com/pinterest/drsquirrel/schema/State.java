package com.pinterest.drsquirrel.schema;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

public class State {
  private static final Logger LOG = LoggerFactory.getLogger(State.class);
  private static final String UNKNOWN_USER = "unknown_user";
  public int maxNumOfLatestExceptions;

  private String cluster;
  private String jobId;
  private String jobName;
  private String applicationId;
  private String username;
  private TreeMap<Date, List<JobException>> criticalExceptions;
  private TreeMap<Date, List<JobException>> regularExceptions;
  private int criticalExceptionsCount = 0;
  private int regularExceptionsCount = 0;
  private long lastUpdatedTimestamp = -1L;

  public State(Signal signal, int maxNumOfLatestExceptions) {
    this.cluster = signal.getCluster();
    this.jobId = signal.getJobId();
    this.jobName = signal.getJobName();
    this.applicationId = signal.getApplicationId();
    this.criticalExceptions = new TreeMap<>();
    this.regularExceptions = new TreeMap<>();
    this.username = UNKNOWN_USER;
    this.maxNumOfLatestExceptions = maxNumOfLatestExceptions;
    this.update(signal);
  }

  public State() {}

  public long getLastUpdatedTimestamp() {
    return lastUpdatedTimestamp;
  }

  public void setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public boolean readyOutput() {
    return applicationId != null && jobId != null;
  }

  /**
   * To merge a State with another State, there are two steps: 1) merge basic info such as cluster,
   * appId, jobId to have least Null values possible 2) merge flink metrics
   *
   * <p>Note that we don't merge state.exceptions because the only usecase we have is merging a
   * state with JM/TM logs with a metric based state. Exceptions _only_ exist in the JM/TM logs, and
   * metric based state only has FlinkMetric to merge into but carries no exceptions.
   */
  public void merge(State state) {
    // merge basic info
    if (cluster == null && state.getCluster() != null) {
      cluster = state.getCluster();
    }
    if (jobId == null && state.getJobId() != null) {
      jobId = state.getJobId();
    }
    if (applicationId == null && state.getApplicationId() != null) {
      applicationId = state.getApplicationId();
    }
    if (jobName == null && state.getJobName() != null) {
      jobName = state.getJobName();
    }
    if (lastUpdatedTimestamp < state.getLastUpdatedTimestamp()) {
      lastUpdatedTimestamp = state.getLastUpdatedTimestamp();
    }
  }

  public FlinkJobInfo toJobInfo(JobState jobState) {
    return new FlinkJobInfo(
        username,
        cluster,
        applicationId,
        jobId,
        jobName,
        getJobExceptionList(),
        jobState.getJobBasicStats(),
        jobState.getCheckpointStats(),
        lastUpdatedTimestamp);
  }

  private List<JobException> getJobExceptionList() {
    List<JobException> result = new ArrayList<>();
    for (List<JobException> jobExceptionList : criticalExceptions.values()) {
      result.addAll(jobExceptionList);
    }
    for (List<JobException> jobExceptionList : regularExceptions.values()) {
      result.addAll(jobExceptionList);
    }
    return result;
  }

  /**
   * To update State with a signal, there are two steps: 1) Update the basic info of the state, such
   * as appId, jobId and JobName 2) Update either this.exceptions or this.metrics based off of the
   * signal type
   */
  public void update(Signal signal) {
    // TODO think of better strategy
    this.lastUpdatedTimestamp = Math.max(this.lastUpdatedTimestamp, signal.getTimestamp());

    // Update basic info of the current job/app
    if (applicationId == null && signal.getApplicationId() != null) {
      applicationId = signal.getApplicationId();
    }
    if (jobId == null && signal.getJobId() != null) {
      jobId = signal.getJobId();
    }
    if (jobName == null && signal.getJobName() != null) {
      jobName = signal.getJobName();
    }

    // Update exceptions or metrics of the current job/app
    if (signal instanceof RawLog) {
      update((RawLog) signal);
    } else if (signal instanceof FlinkMetric) {
      update((FlinkMetric) signal);
    } else {
      throw new UnsupportedOperationException("Not able to find Signal class");
    }
  }

  private void update(RawLog rawLog) {
    Preconditions.checkState(rawLog.getUsername() != null);
    this.username = rawLog.getUsername();

    JobException jobException =
        new JobException(
            rawLog.getException(),
            rawLog.getHostName(),
            rawLog.getContainerId(),
            rawLog.getFile(),
            rawLog.getTimestamp());
    if (rawLog.getFile().contains("ExecutionGraph")) {
      criticalExceptions.putIfAbsent(rawLog.getDate(), new LinkedList<>());
      criticalExceptions.get(rawLog.getDate()).add(jobException);
      criticalExceptionsCount++;
    } else {
      regularExceptions.putIfAbsent(rawLog.getDate(), new LinkedList<>());
      regularExceptions.get(rawLog.getDate()).add(jobException);
      regularExceptionsCount++;
    }

    if (criticalExceptionsCount > maxNumOfLatestExceptions) {
      removeEarliestException(criticalExceptions);
      criticalExceptionsCount--;
    }

    if (regularExceptionsCount > maxNumOfLatestExceptions) {
      removeEarliestException(regularExceptions);
      regularExceptionsCount--;
    }
  }

  // TODO aggregate Operator metrics
  private void update(FlinkMetric metric) {}

  private void removeEarliestException(TreeMap<Date, List<JobException>> exceptionMap) {
    Date firstTimestamp = exceptionMap.firstKey();
    exceptionMap.get(firstTimestamp).remove(0);
    if (exceptionMap.get(firstTimestamp).size() == 0) {
      exceptionMap.remove(firstTimestamp);
    }
  }

  public int getMaxNumOfLatestExceptions() {
    return maxNumOfLatestExceptions;
  }

  public void setMaxNumOfLatestExceptions(int maxNumOfLatestExceptions) {
    this.maxNumOfLatestExceptions = maxNumOfLatestExceptions;
  }

  public TreeMap<Date, List<JobException>> getCriticalExceptions() {
    return criticalExceptions;
  }

  public void setCriticalExceptions(TreeMap<Date, List<JobException>> criticalExceptions) {
    this.criticalExceptions = criticalExceptions;
  }

  public TreeMap<Date, List<JobException>> getRegularExceptions() {
    return regularExceptions;
  }

  public void setRegularExceptions(TreeMap<Date, List<JobException>> regularExceptions) {
    this.regularExceptions = regularExceptions;
  }

  public int getCriticalExceptionsCount() {
    return criticalExceptionsCount;
  }

  public void setCriticalExceptionsCount(int criticalExceptionsCount) {
    this.criticalExceptionsCount = criticalExceptionsCount;
  }

  public int getRegularExceptionsCount() {
    return regularExceptionsCount;
  }

  public void setRegularExceptionsCount(int regularExceptionsCount) {
    this.regularExceptionsCount = regularExceptionsCount;
  }
}
