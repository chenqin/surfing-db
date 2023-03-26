package com.pinterest.drsquirrel.schema;

import io.airlift.units.Duration;

import java.util.Objects;

public class JobBasicStats {
  private final Duration jobUptime;
  private final int jobFullRestarts;

  public JobBasicStats(Duration jobUptime, int jobFullRestarts) {
    this.jobUptime = jobUptime;
    this.jobFullRestarts = jobFullRestarts;
  }

  public Duration getJobUptime() {
    return jobUptime;
  }

  public int getJobFullRestarts() {
    return jobFullRestarts;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobBasicStats that = (JobBasicStats) o;
    return jobFullRestarts == that.jobFullRestarts
        && jobUptime.equals(that.jobUptime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobUptime, jobFullRestarts);
  }

  @Override
  public String toString() {
    return "JobBasicStats{"
        + "jobUptime=" + jobUptime
        + ", jobFullRestarts=" + jobFullRestarts + '}';
  }
}
