package org.surfing.drsquirrel.schema;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RawLog implements Signal, Cloneable, Serializable {
  private String cluster;
  private String hostName;
  private String applicationId;
  private String containerId;
  private String username;

  private String jobId;
  private String jobName;
  private long timestamp;

  private String saltKey;

  private SignalType signalType;

  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
  private Date date;

  private String level;
  private String file; // eg. TaskManager:36
  private String exception;

  public RawLog() {}

  public RawLog(
      String username,
      String cluster,
      String hostName,
      String applicationId,
      String containerId,
      Date date,
      String level,
      String file,
      String exception) {
    this.username = requireNonNull(username, "username is null");
    this.cluster = requireNonNull(cluster, "cluster is null");
    this.hostName = requireNonNull(hostName, "hostName is null");
    this.applicationId = requireNonNull(applicationId, "applicationId is null");
    this.containerId = requireNonNull(containerId, "containerId is null");
    this.date = requireNonNull(date, "timestamp is null");
    this.level = requireNonNull(level.trim(), "level is null");
    this.file = requireNonNull(file.trim(), "file is null");
    this.exception = requireNonNull(exception.trim(), "exception is null");
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  @Override
  public long getTimestamp() {
    return date.getTime();
  }

  @Override
  public String getSaltKey() {
    return String.format("%s:%d", applicationId, Objects.hash(containerId, applicationId) % 100);
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  @Override
  public SignalType getSignalType() {
    return SignalType.JOB_LOG;
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
    return null;
  }

  @Override
  public String getJobName() {
    return null;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }
}
