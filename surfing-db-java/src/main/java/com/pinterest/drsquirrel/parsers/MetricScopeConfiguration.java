package com.pinterest.drsquirrel.parsers;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * metricsScope:
 *   jm: flink.jm._t_host.<host>
 *   jm.job: flink.jm._t_host.<host>._t_job_id.<job_id>._t_job_name.<job_name>
 *   operator: flink.operator._t_host.<host>._t_tm_id.<tm_id>._t_job_id.<job_id>._t_job_name
 *   .<job_name>._t_operator_id.<operator_id>._t_operator_name.<operator_name>._t_subtask_index
 *   .<subtask_index>
 *   task: flink.task._t_host.<host>._t_tm_id.<tm_id>._t_job_id.<job_id>._t_job_name.<job_name>
 *     ._t_task_id.<task_id>._t_task_name.<task_name>._t_task_attempt_id.<task_attempt_id>
 *       ._t_task_attempt_num.<task_attempt_num>._t_subtask_index.<subtask_index>
 *   tm: flink.tm._t_host.<host>._t_tm_id.<tm_id>
 *   tm.job: flink.tm._t_host.<host>._t_tm_id.<tm_id>._t_job_id.<job_id>._t_job_name.<job_name>
 */
public class MetricScopeConfiguration {

  private String jm;  // job manager metrics scope

  private String jmJob;

  private String jmWithAppId; // job manager metrics scope with app Id

  private String jmJobWithAppId;

  private String operator;

  private String task;

  private String tm;

  private String tmJob;


  @JsonProperty
  public String getJm() {
    return jm;
  }

  @JsonProperty
  public void setJm(String jm) {
    this.jm = jm;
  }

  @JsonProperty
  public String getJmJob() {
    return jmJob;
  }

  @JsonProperty
  public void setJmJob(String jmJob) {
    this.jmJob = jmJob;
  }

  @JsonProperty
  public void setOperator(String operator) {
    this.operator = operator;
  }

  @JsonProperty
  public String getOperator() {
    return operator;
  }

  @JsonProperty
  public void setTask(String task) {
    this.task = task;
  }

  @JsonProperty
  public String getTask() {
    return task;
  }


  @JsonProperty
  public void setTm(String tm) {
    this.tm = tm;
  }

  @JsonProperty
  public String getTm() {
    return tm;
  }

  @JsonProperty
  public void setTmJob(String tmJob) {
    this.tmJob = tmJob;
  }

  @JsonProperty
  public String getTmJob() {
    return tmJob;
  }

  @JsonProperty
  public String getJmWithAppId() {
    return jmWithAppId;
  }

  @JsonProperty
  public void setJmWithAppId(String jmWithAppId) {
    this.jmWithAppId = jmWithAppId;
  }

  @JsonProperty
  public String getJmJobWithAppId() {
    return jmJobWithAppId;
  }

  @JsonProperty
  public void setJmJobWithAppId(String jmJobWithAppId) {
    this.jmJobWithAppId = jmJobWithAppId;
  }
}
