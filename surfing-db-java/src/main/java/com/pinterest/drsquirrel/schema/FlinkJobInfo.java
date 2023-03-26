package com.pinterest.drsquirrel.schema;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * TODO the exact same class is in drSquirrel server repo as well.
 * Need to keep one copy only later.
 */
public class FlinkJobInfo {
  private final String username;
  private final String cluster;
  private final String applicationId;
  private final String jobId;
  private final String jobName;
  //private final Set<String> allMetricNames;
  private final List<JobException> exceptions;

  private final JobBasicStats jobBasicStats;
  private final CheckpointStats checkpointStats;
  private final long lastUpdatedTimestamp;

  public FlinkJobInfo(String username,
                      String cluster,
                      String applicationId,
                      String jobId,
                      String jobName,
                      //Set<String> allMetricNames,
                      List<JobException> exceptions,
                      JobBasicStats jobBasicStats,
                      CheckpointStats checkpointStats,
                      long lastUpdatedTimestamp
                      ) {
    this.username = requireNonNull(username, "username is null");
    this.cluster = requireNonNull(cluster, "cluster is null");
    this.applicationId = requireNonNull(applicationId, "applicationId is null");
    this.jobId = requireNonNull(jobId, "jobId is null");
    this.jobName = requireNonNull(jobName, "jobName is null");
    //this.allMetricNames = allMetricNames;
    this.exceptions = exceptions;
    this.jobBasicStats = jobBasicStats;
    this.checkpointStats = checkpointStats;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }


  public String getUsername() {
    return username;
  }

  public String getCluster() {
    return cluster;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getJobId() {
    return jobId;
  }

  public List<JobException> getExceptions() {
    return exceptions;
  }

//  public Set<String> getAllMetricNames() {
//    return allMetricNames;
//  }

  public String getJobName() {
    return jobName;
  }

  public JobBasicStats getJobBasicStats() {
    return jobBasicStats;
  }

  public CheckpointStats getCheckpointStats() {
    return checkpointStats;
  }

  public long getLastUpdatedTimestamp() {
    return lastUpdatedTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlinkJobInfo that = (FlinkJobInfo) o;
    return lastUpdatedTimestamp == that.lastUpdatedTimestamp
        && username.equals(that.username)
        && cluster.equals(that.cluster)
        && applicationId.equals(that.applicationId)
        && jobId.equals(that.jobId)
        && jobName.equals(that.jobName)
        && exceptions.equals(that.exceptions)
        && jobBasicStats.equals(that.jobBasicStats)
        && checkpointStats.equals(that.checkpointStats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, applicationId, jobId, jobName);
  }
}
