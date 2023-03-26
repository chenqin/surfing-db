package com.pinterest.drsquirrel.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.drsquirrel.schema.FlinkJobInfo;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.RawLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;

public class DrSquirrelUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DrSquirrelUtils.class);
  // TODO Relax the condition to support full class name
  private static Pattern LOG_PATTERN = Pattern.compile(
      "([\\w\\.]*) (xenon-[a-z0-9\\-]+) (application_[a-z0-9_]+) (container_[a-z0-9_]+)"
          + " (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) ([A-Z ]+) (\\S+:\\S+?) (.*)",
      Pattern.DOTALL);

  /**
   * Not all logs contains userName. Remove this once all log conform to LOG_PATTERN.
   */
  private static Pattern LOG_OLD_PATTERN = Pattern.compile(
      "(xenon-[a-z0-9\\-]+) (application_[a-z0-9_]+) (container_[a-z0-9_]+)"
          + " (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) ([A-Z ]+) (\\S+:\\S+?) (.*)",
      Pattern.DOTALL);


  public static String CLUSTER_PATTERN = "(xenon(-pii)?-[a-z]+-[0-9]+)-.*";
  public static ObjectMapper MAPPER = new ObjectMapper();

  public static FlinkMetricParser metricParser = new FlinkMetricParser();

  public static RawLog constructRawLog(String logline, String sourceTopic) throws Exception {
    Matcher m = LOG_PATTERN.matcher(logline);

    if (m.find()) {
      String username = m.group(1).equals("") ? "unknown_user" : m.group(1);
      String cluster = deriveClusterNameFromHost(m.group(2));
      Date timestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(m.group(5));

      RawLog rawLog = new RawLog(
          username, cluster, m.group(2), m.group(3), m.group(4),
          timestamp, m.group(6), m.group(7), m.group(8)
      );
      checkState(rawLog.getApplicationId() != null && rawLog.getJobId() == null);
      return rawLog;
    } else {
      m = LOG_OLD_PATTERN.matcher(logline);
      checkState(m.find(), "Pattern not found. Source: " + sourceTopic + " Log: " + logline);
      String cluster = deriveClusterNameFromHost(m.group(1));
      Date timestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(m.group(4));
      RawLog rawLog = new RawLog("unknown_user",
          cluster, m.group(1), m.group(2), m.group(3),
          timestamp, m.group(5), m.group(6), m.group(7));
      checkState(rawLog.getApplicationId() != null && rawLog.getJobId() == null);
      return rawLog;
    }
  }

  public static String deriveClusterNameFromHost(String host) {
    Pattern r = Pattern.compile(CLUSTER_PATTERN);
    Matcher m = r.matcher(host);
    checkState(m.find(), "Cannot derive cluster name from host. Host: " + host);
    return m.group(1);
  }

  public static String serializeJobInfo(FlinkJobInfo d) throws JsonProcessingException {
    String serializedJobInfo = MAPPER.writeValueAsString(d);
    if (serializedJobInfo.length() > 6000000) {
      LOG.warn(String.format(
          "Length of the serialized FlinkJobInfo is %s which is too large that "
              + "it risks exceeding kafka max request size."
              + " Length of serialized exception list: %s,"
              + " Application id: %s,  Job name: %s, Cluster: %s",
          serializedJobInfo.length(), MAPPER.writeValueAsString(d.getExceptions()).length(),
          d.getApplicationId(), d.getJobName(), d.getCluster())
      );
      return "";
    }
    return serializedJobInfo;
  }

  public static FlinkMetric constructFlinkMetric(String metricJson) throws Exception {
    FlinkMetric flinkMetric = metricParser.parseMetricJsonString(metricJson);
    return flinkMetric;
  }
}
