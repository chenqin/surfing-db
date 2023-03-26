package com.pinterest.drsquirrel.schema;

import static java.util.Objects.requireNonNull;

public class JobException {
  private final String message;
  private final String hostName;
  private final String containerId;
  private final String file;
  private final long timestamp;

  public JobException(String message, String hostName, String containerId, String file,
                      long timestamp) {
    this.message = requireNonNull(message.trim(), "message is null");
    this.hostName = requireNonNull(hostName, "hostName is null");
    this.containerId = requireNonNull(containerId, "containerId is null");
    this.file = requireNonNull(file, "file is null");
    this.timestamp = requireNonNull(timestamp, "timestamp is null");
  }

  public String getMessage() {
    return message;
  }

  public String getHostName() {
    return hostName;
  }

  public String getContainerId() {
    return containerId;
  }

  public String getFile() {
    return file;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
