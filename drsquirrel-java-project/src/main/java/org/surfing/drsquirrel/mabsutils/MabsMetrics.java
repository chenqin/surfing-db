package org.surfing.drsquirrel.mabsutils;

import java.util.HashMap;
import java.util.Map;

public class MabsMetrics {
  public long timestamp;
  public String service_tags;
  public String node_tags;

  public String service_name;

  public Map<String, Long> counters = new HashMap<>();
  public Map<String, String> hist = new HashMap<>();

  public Map<String, Double> gauge = new HashMap<>();

  public Map<String, Double> doubleconunters = new HashMap<>();
  public Long getTimestamp() {
    return timestamp;
  }
  
  public String getService_tags() {
    return service_tags;
  }

  public String getNode_tags() {
    return node_tags;
  }

  public Map<String, Long> getCounters() {
    return counters;
  }
  
  public Map<String, String> getHistograms() {
    return hist;
  }

  public Map<String, Double> getGauges() {
    return gauge;
  }

  public Map<String, Double> getDouble_counters() {
    return doubleconunters;
  }

  @Override
  public String toString() {
    return "MabsMetrics{" +
        "timestamp=" + timestamp +
        ", service_tags='" + service_tags + '\'' +
        ", node_tags='" + node_tags + '\'' +
        ", service_name='" + service_name + '\'' +
        ", counters=" + counters +
        ", hist=" + hist +
        ", gauge=" + gauge +
        ", doubleconunters=" + doubleconunters +
        '}';
  }
}
