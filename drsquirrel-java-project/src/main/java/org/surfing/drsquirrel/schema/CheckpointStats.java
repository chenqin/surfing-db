package org.surfing.drsquirrel.schema;

import io.airlift.units.Duration;

import java.util.Map;
import java.util.Objects;

public class CheckpointStats {
  private final int failedCheckpoints;
  private final long size;
  private final Duration durationInMs;
  private final Map<Long, String> checkpointPaths;

  public CheckpointStats(
      int failedCheckpoints, long size, Duration durationInMs, Map<Long, String> checkpointPaths) {
    this.failedCheckpoints = failedCheckpoints;
    this.size = size;
    this.durationInMs = durationInMs;
    this.checkpointPaths = checkpointPaths;
  }

  public int getFailedCheckpoints() {
    return failedCheckpoints;
  }

  public long getSize() {
    return size;
  }

  public Duration getDurationInMs() {
    return durationInMs;
  }

  public Map<Long, String> getCheckpointPaths() {
    return checkpointPaths;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CheckpointStats that = (CheckpointStats) o;
    return failedCheckpoints == that.failedCheckpoints
        && size == that.size
        && Objects.equals(durationInMs, that.durationInMs)
        && Objects.equals(checkpointPaths, that.checkpointPaths);
  }

  @Override
  public int hashCode() {
    return Objects.hash(failedCheckpoints, size, durationInMs, checkpointPaths);
  }

  @Override
  public String toString() {
    return "CheckpointStats{"
        + "failedCheckpoints="
        + failedCheckpoints
        + ", size="
        + size
        + ", durationInMs="
        + durationInMs
        + '}';
  }
}
