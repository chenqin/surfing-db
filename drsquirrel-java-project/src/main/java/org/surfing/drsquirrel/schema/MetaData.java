package org.surfing.drsquirrel.schema;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Objects.requireNonNull;

public class MetaData {
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
  private final String hostName;
  private final String containerId;
  private final Date timestamp;
  private final String file;

  public MetaData(String hostName, String containerId, Date timestamp, String file) {
    this.hostName = requireNonNull(hostName, "hostName is null");
    this.containerId = requireNonNull(containerId, "containerId is null");
    this.timestamp = requireNonNull(timestamp, "timestamp is null");
    this.file = requireNonNull(file, "file is null");
  }

  public String getHostName() {
    return hostName;
  }

  public String getContainerId() {
    return containerId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public String getFile() {
    return file;
  }

  @Override
  public String toString() {
    String formattedTimestamp = dateFormat.format(timestamp);
    return String.format("%s %s %s %s", hostName, containerId, formattedTimestamp, file);
  }
}
