package com.pinterest.drsquirrel.parsers;

import com.google.common.collect.ImmutableSet;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.FlinkMetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkMetricScopeParser {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricScopeParser.class);
  private static final String APPLICATION_ID_VAR = "<application_id>";
  private static final String HOST_VAR = "<host>";
  private static final String JOB_ID_VAR = "<job_id>";
  private static final String JOB_NAME_VAR = "<job_name>";
  private static final String OPERATOR_ID_VAR = "<operator_id>";
  private static final String OPERATOR_NAME_VAR = "<operator_name>";

  private static final String SUBTASK_INDEX_VAR = "<subtask_index>";
  private static final String TASK_ID_VAR = "<task_id>";
  private static final String TASK_NAME_VAR = "<task_name>";
  private static final String TASK_ATTEMPT_ID_VAR = "<task_attempt_id>";
  private static final String TASK_ATTEMPT_NUM_VAR = "<task_attempt_num>";
  private static final String TM_ID_VAR = "<tm_id>";

  private static final Set<String> METRIC_SCOPE_VARS =
      ImmutableSet.of(
          APPLICATION_ID_VAR,
          HOST_VAR,
          JOB_ID_VAR,
          JOB_NAME_VAR,
          OPERATOR_ID_VAR,
          OPERATOR_NAME_VAR,
          SUBTASK_INDEX_VAR,
          TASK_ID_VAR,
          TASK_NAME_VAR,
          TASK_ATTEMPT_ID_VAR,
          TASK_ATTEMPT_NUM_VAR,
          TM_ID_VAR);

  private FlinkMetricType metricType;
  private String regexStr;
  private Pattern pattern;
  private List<String> scopeVars;

  public FlinkMetricScopeParser(FlinkMetricType metricType, String scopeStr) {
    this.metricType = metricType;
    this.scopeVars = getScopeVars(scopeStr);
    this.regexStr = getMetricScopeRegexString(scopeStr);
    this.pattern = Pattern.compile(regexStr);
  }

  public boolean isMetricType(String metricStr) {
    Matcher matcher = pattern.matcher(metricStr);
    return matcher.matches();
  }

  FlinkMetricType getMetricType() {
    return this.metricType;
  }

  public FlinkMetric parseMetricNameString(String metricStr) {
    Matcher matcher = pattern.matcher(metricStr);
    FlinkMetric metric = new FlinkMetric();
    metric.setType(metricType);
    if (matcher.find()) {
      for (int i = 1; i <= scopeVars.size(); i++) {
        String str = matcher.group(i);
        String var = scopeVars.get(i - 1);
        switch (var) {
          case APPLICATION_ID_VAR:
            metric.setApplicationId(str);
            break;

          case HOST_VAR:
            metric.setHost(str);
            break;

          case JOB_ID_VAR:
            metric.setJobId(str);
            break;

          case JOB_NAME_VAR:
            metric.setJobName(str);
            break;

          case OPERATOR_ID_VAR:
            metric.setOperatorId(str);
            break;

          case OPERATOR_NAME_VAR:
            metric.setOperatorName(str);
            break;

          case SUBTASK_INDEX_VAR:
            metric.setSubtaskIndex(Integer.parseInt(str));
            break;

          case TASK_ID_VAR:
            metric.setTaskId(str);
            break;

          case TASK_NAME_VAR:
            metric.setTaskName(str);
            break;

          case TASK_ATTEMPT_ID_VAR:
            metric.setTaskAttemptId(str);
            break;

          case TASK_ATTEMPT_NUM_VAR:
            metric.setTaskAttemptNum(Integer.parseInt(str));
            break;

          case TM_ID_VAR:
            metric.setTaskManagerId(str);
            break;

          default:
            LOG.warn("Unexpected scope var : " + var);
        }
      }
      String str = matcher.group(scopeVars.size() + 1);
      metric.setName(str);
      if (metric.getTaskManagerId() != null) {
        String taskManagerId = metric.getTaskManagerId();
        metric.setApplicationId(deriveAppIdFromTaskManagerId(taskManagerId));
      }
    }
    return metric;
  }

  /**
   * applicationId is part of taskManagerId, this functions is used to
   * derive applicationID from taskManagerId.
   *
   * taskManagerId has a format of
   * "container_epoch_appId_attemptId_containerId" with epoch being optional
   * Example:
   * - container_e02_1599158147594_70875_01_000031  (has epoch)
   * - container_1598646788314_76489_01_000008      (without epoch)
   */
  public static String deriveAppIdFromTaskManagerId(String taskManagerId) {
    String[] strs = taskManagerId.split("_");
    if (strs.length < 4) {
      return null;
    }
    return String.format(
        "application_%s_%s", strs[strs.length - 4], strs[strs.length - 3]);
  }

  public static String getMetricScopeRegexString(String scopeStr) {
    String result = scopeStr.replace(".", "\\.");
    for (String scopeVar : METRIC_SCOPE_VARS) {
      String regex;
      if (scopeVar.equals(SUBTASK_INDEX_VAR)) {
        regex = "(\\d+)?";
      } else if (scopeVar.equals(TM_ID_VAR) || scopeVar.equals(JOB_ID_VAR)) {
        regex = "([a-zA-Z0-9\\_]+)?";
      } else {
        regex = "(.*?)";
      }
      result = result.replace(scopeVar, regex);
    }
    result += "\\.(.*)";
    return result;
  }


  public static List<String> getScopeVars(String scopeStr) {
    List<String> varsSequence = new ArrayList<>();
    Pattern pattern = Pattern.compile("<[a-zA-Z_]*?>");
    Matcher matcher = pattern.matcher(scopeStr);
    while (matcher.find()) {
      String group = matcher.group();
      if (METRIC_SCOPE_VARS.contains(group)) {
        varsSequence.add(group);
      }
    }
    return varsSequence;
  }
}
