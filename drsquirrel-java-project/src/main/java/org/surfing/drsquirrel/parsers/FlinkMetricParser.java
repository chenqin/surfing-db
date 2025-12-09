package org.surfing.drsquirrel.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.surfing.drsquirrel.schema.FlinkMetric;
import org.surfing.drsquirrel.schema.FlinkMetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkMetricParser {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricParser.class);

  // TODO: move these scopes to property file
  private static final String SCOPE_OPERATOR =
      "flink.operator._t_host.<host>._t_tm_id.<tm_id>._t_job_id.<job_id>"
          + "._t_job_name.<job_name>._t_operator_id.<operator_id>._t_operator_name"
          + ".<operator_name>._t_subtask_index.<subtask_index>";

  private static final String SCOPE_TASK =
      "flink.task._t_host.<host>._t_tm_id.<tm_id>._t_job_id.<job_id>._t_job_name.<job_name>"
          + "._t_task_id.<task_id>._t_task_name.<task_name>._t_task_attempt_id.<task_attempt_id>"
          + "._t_task_attempt_num.<task_attempt_num>._t_subtask_index.<subtask_index>";

  private static final String SCOPE_TM = "flink.tm._t_host.<host>._t_tm_id.<tm_id>";

  private static final String SCOPE_TMJOB =
      "flink.tm._t_host.<host>._t_tm_id.<tm_id>._t_job_id.<job_id>._t_job_name.<job_name>";

  private static final String SCOPE_JM_WITH_APPID =
      "flink.jm._t_application_id.<application_id>._t_host.<host>";

  private static final String SCOPE_JM_JOB_WITH_APPID =
      "flink.jm._t_application_id.<application_id>._t_host.<host>._t_job_id.<job_id>._t_job_name"
          + ".<job_name>";

  private static final String SCOPE_JM = "flink.jm._t_host.<host>";

  private static final String SCOPE_JM_JOB =
      "flink.jm._t_host.<host>._t_job_id.<job_id>._t_job_name" + ".<job_name>";
  private static final MetricScopeConfiguration scopeConfiguration = getScopeConfiguration();

  // important: the order of FlinkMetricScopeParser in the list cannot be changed.
  private static final List<FlinkMetricType> METRIC_TYPE_EXAM_SEQUENCE =
      ImmutableList.of(
          FlinkMetricType.JOBMANAGER_JOB_WITH_APPID,
          FlinkMetricType.JOBMANAGER_WITH_APPID,
          FlinkMetricType.JOBMANAGER_JOB,
          FlinkMetricType.JOBMANAGER,
          FlinkMetricType.TASKMANAGER_JOB_LATENCY,
          FlinkMetricType.TASKMANAGER_JOB,
          FlinkMetricType.TASKMANAGER,
          FlinkMetricType.OPERATOR,
          FlinkMetricType.TASK);

  private ObjectMapper objectMapper = new ObjectMapper();

  private Map<FlinkMetricType, FlinkMetricScopeParser> metricScopeParsers;

  public FlinkMetricParser() {
    metricScopeParsers = new HashMap<>();

    metricScopeParsers.put(
        FlinkMetricType.JOBMANAGER_JOB,
        new FlinkMetricScopeParser(FlinkMetricType.JOBMANAGER_JOB, scopeConfiguration.getJmJob()));
    metricScopeParsers.put(
        FlinkMetricType.JOBMANAGER,
        new FlinkMetricScopeParser(FlinkMetricType.JOBMANAGER, scopeConfiguration.getJm()));
    metricScopeParsers.put(
        FlinkMetricType.JOBMANAGER_JOB_WITH_APPID,
        new FlinkMetricScopeParser(
            FlinkMetricType.JOBMANAGER_JOB_WITH_APPID, scopeConfiguration.getJmJobWithAppId()));
    metricScopeParsers.put(
        FlinkMetricType.JOBMANAGER_WITH_APPID,
        new FlinkMetricScopeParser(
            FlinkMetricType.JOBMANAGER_WITH_APPID, scopeConfiguration.getJmWithAppId()));
    String scopeTmJobLatency = getTmJobLatencyPattern(scopeConfiguration.getTmJob());
    metricScopeParsers.put(
        FlinkMetricType.TASKMANAGER_JOB_LATENCY,
        new FlinkMetricScopeParser(FlinkMetricType.TASKMANAGER_JOB_LATENCY, scopeTmJobLatency));
    metricScopeParsers.put(
        FlinkMetricType.TASKMANAGER_JOB,
        new FlinkMetricScopeParser(FlinkMetricType.TASKMANAGER_JOB, scopeConfiguration.getTmJob()));
    metricScopeParsers.put(
        FlinkMetricType.TASKMANAGER,
        new FlinkMetricScopeParser(FlinkMetricType.TASKMANAGER, scopeConfiguration.getTm()));
    metricScopeParsers.put(
        FlinkMetricType.OPERATOR,
        new FlinkMetricScopeParser(FlinkMetricType.OPERATOR, scopeConfiguration.getOperator()));
    metricScopeParsers.put(
        FlinkMetricType.TASK,
        new FlinkMetricScopeParser(FlinkMetricType.TASK, scopeConfiguration.getTask()));
  }

  private static MetricScopeConfiguration getScopeConfiguration() {
    MetricScopeConfiguration scopeConfiguration = new MetricScopeConfiguration();
    scopeConfiguration.setJm(SCOPE_JM);
    scopeConfiguration.setJmJob(SCOPE_JM_JOB);
    scopeConfiguration.setJmWithAppId(SCOPE_JM_WITH_APPID);
    scopeConfiguration.setJmJobWithAppId(SCOPE_JM_JOB_WITH_APPID);
    scopeConfiguration.setOperator(SCOPE_OPERATOR);
    scopeConfiguration.setTask(SCOPE_TASK);
    scopeConfiguration.setTm(SCOPE_TM);
    scopeConfiguration.setTmJob(SCOPE_TMJOB);
    return scopeConfiguration;
  }

  private static String getTmJobLatencyPattern(String scopeTmJob) {
    return scopeTmJob
        + ".latency.source_id.<operator_id>.operator_id.<operator_id>.operator_subtask_index"
        + ".<subtask_index>";
  }

  public FlinkMetric parseMetricJsonString(String jsonString) throws Exception {
    try {
      JsonNode jsonNode = objectMapper.readTree(jsonString);
      long timestamp = jsonNode.get("timestamp").asLong();
      String host = jsonNode.get("host").asText();
      String metricName = jsonNode.get("metricName").asText();
      String metricValue = jsonNode.get("metricValue").asText();

      FlinkMetric metric = parseMetricNameString(metricName);
      metric.setTimestamp(timestamp);
      metric.setHost(host);
      metric.setFullName(metricName);
      metric.setValue(metricValue);
      metric.setCluster(DrSquirrelUtils.deriveClusterNameFromHost(host));
      return metric;
    } catch (Exception e) {
      LOG.debug(e.getMessage());
      return null;
    }
  }

  public FlinkMetric parseMetricNameString(String metricName) {
    FlinkMetricType metricType = getMetricType(metricName);
    FlinkMetricScopeParser scopeParser = metricScopeParsers.getOrDefault(metricType, null);
    if (scopeParser == null) {
      LOG.error("Failed to find the scope parser for metric " + metricName);
      return null;
    }
    FlinkMetric metric = scopeParser.parseMetricNameString(metricName);
    return metric;
  }

  public FlinkMetricType getMetricType(String metricStr) {
    for (FlinkMetricType metricType : METRIC_TYPE_EXAM_SEQUENCE) {
      if (metricScopeParsers.get(metricType).isMetricType(metricStr)) {
        return metricScopeParsers.get(metricType).getMetricType();
      }
    }
    return FlinkMetricType.UNKNOWN;
  }
}
