/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <chrono>
#include <fmt/core.h>
#include <future>
#include <glog/logging.h>
#include <iostream>
#include <omp.h>
#include <rapidjson/document.h>
#include <string>
#include "connector/kafka.h"
#include "engine/engine.h"
#include "meta/node.h"
#include "table/processors.h"
#include "table/utils.h"

#define FLUSH_DIR "/tmp/"

using namespace surfingdb::table::schema;
using surfingdb::meta::node;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std::chrono;
using namespace surfingdb::engine;
namespace cp = ::arrow::compute;

volatile std::sig_atomic_t terminal_signal;
void signal_handler(int signal) {
  std::cout << "user exit program";
  terminal_signal = signal;
}
/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main(int argc, char** argv) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);

  //  create node of cluster
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  /**
   * @brief define topic schema
   *
   */
  RowSchema r;
  SchemaUtils::initField(r, "topic", RowType::STRING, 64);
  SchemaUtils::initField(r, "payload", RowType::STRING, MAX_STR_LEN);
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);

  /**
   * pull every 2 seconds
   */
  int batch = 1000;
  int interval = 100;
  int world = node->world;

  size_t total = 0;
  srand(std::time(nullptr));

  auto start = MPI_Wtime();
  std::string group_id = "cqin-test";

  std::signal(SIGTERM | SIGINT, signal_handler);

  auto metric_deser = [](const char* payload, const mschema& out) {
    auto r = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value v[2];
    v[0].p_val.string_val = "metric";
    v[1].p_val.string_val = std::string(payload);
    for (int i = 0; i < 2; i++) {
      r->write(out.fields.at(i), v[i]);
    }
    return r;
  };
  auto metrics_prod = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr,
    { "xenon_metrics_prod" }, "/var/serverset/discovery.datakafka08.prod", group_id, false);
  metrics_prod.setDeser(metric_deser);

  auto metrics_staging = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr,
    { "xenon_metrics_staging" }, "/var/serverset/discovery.datakafka08.prod", group_id, false);
  metrics_staging.setDeser(metric_deser);

  auto metric_log_deser = [](const char* payload, const mschema& out) {
    auto r = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value v[2];
    v[0].p_val.string_val = "log";
    v[1].p_val.string_val = std::string(payload);
    for (int i = 0; i < 2; i++) {
      r->write(out.fields.at(i), v[i]);
    }
    return r;
  };
  auto metrics_log_staging = KafkaConnector(
    node, "kafka-source", batch / 10, interval, schema_ptr,
    { "xenon-logs-staging" }, "/var/serverset/discovery.metricskafka07.prod", group_id, false);
  metrics_log_staging.setDeser(metric_log_deser);

  auto metrics_log_prod = KafkaConnector(
    node, "kafka-source", batch / 10, interval, schema_ptr,
    { "xenon-logs-prod" }, "/var/serverset/discovery.metricskafka07.prod", group_id, false);
  metrics_log_prod.setDeser(metric_log_deser);

  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    long local_row_count = 0;
    // kafka consumer

    auto t1 = std::async(std::launch::async, [&metrics_prod] {
      RowSchema r;
      SchemaUtils::initField(r, "topic", RowType::STRING, 64);
      SchemaUtils::initField(r, "payload", RowType::STRING, MAX_STR_LEN);
      const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);
      return metrics_prod.consume_batch([&schema_ptr](const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        PValue v1, v2;
        Value placeholder;
        v1.string_val = "metric";
        v2.string_val = std::string(payload);
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder);
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
      });
    });

    auto t2 = std::async(std::launch::async, [&metrics_log_prod] {
      RowSchema r;
      SchemaUtils::initField(r, "topic", RowType::STRING, 64);
      SchemaUtils::initField(r, "payload", RowType::STRING, MAX_STR_LEN);
      const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);
      return metrics_log_prod.consume_batch([&schema_ptr](const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        PValue v1, v2;
        Value placeholder;
        v1.string_val = "log";
        v2.string_val = std::string(payload);
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder);
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
      });
    });
    auto metric_table = t1.get();
    local_row_count += metric_table->num_rows();
    auto log_table = t2.get();
    local_row_count += log_table->num_rows();
    MPI_Barrier(MPI_COMM_WORLD);
    /*
      step 1, get entire raw streams, metric and logs
    */
    auto metric = engine::source(metric_table);
    auto log = engine::source(log_table);
    /*
    * step 2, union raw streams
     DataStream<Signal> signals = allMetrics.union(exceptions);
    */
    auto metric_log = engine::union_op(metric, log);

    /*
    * step 3, clean up raw steams, extract signals
    DataStream<Signal> allMetrics = metricsStream.filter( m -> m.getJobId() != null)
        .keyBy(FlinkMetric::getJobId)
        .process(new MetricProcessor())
        .name("metric-processor")
        .setParallelism(parseInt(configuration.getJavaString("metric-processor.parallelism")));

    SingleOutputStreamOperator<Signal> exceptions = logStream.keyBy(RawLog::getSaltKey)
        .process(new RawLogProcessor())
        .name("rawLog-processor")
        .setParallelism(parseInt(configuration.getJavaString("rawLog-processor.parallelism")));
    */
    auto signal = engine::java(metric_log, "CleanupWrapper", node);
    
    /*
    * step 4, shuffle by applicationId
    */
    auto app_signals = engine::shuffle(
      signal, "appid", [](size_t hash, int rank, int workers) { return hash % workers; }, false, node);
    /*
    * step 5, aggregate signals as application state by applicationId, pass through metric signals

     // Signals with appId (i.e. RawLog + Part of Flink System Metrics that have tmId/appId)
    DataStream<Signal> appIdSignalStream = signals.rebalance().filter(
        signal -> signal.getApplicationId() != null
            && !signal.getApplicationId().equals("application_unknown"))
        .name("filter-has-AppId")
        .setParallelism(parseInt(configuration.getJavaString("filter-has-AppId.parallelism")));

    int stateTtlInDays = parseInt(configuration.getJavaString("state.ttl.days"));
    int maxNumOfLatestExceptions =
        parseInt(configuration.getJavaString("num-of-latest-exceptions.max"));

    // Transform signals with appId to State group by AppId
    DataStream<State> appIdStates = appIdSignalStream
        .keyBy(Signal::getApplicationId)
        .process(new SignalsToStateProcessor(stateTtlInDays, maxNumOfLatestExceptions))
        .setParallelism(parseInt(configuration.getJavaString("group-by-AppId.parallelism")));

    // Signals with _only_ jobId (i.e. Flink System Metrics - jm.job)
    DataStream<Signal> jobIdSignalStream =
        signals
            .rebalance()
            .filter(signal -> signal.getJobId() != null)
            .name("filter-has-JobId")
            .setParallelism(parseInt(configuration.getJavaString("filter-has-JobId.parallelism")));
    */
    auto app_state = engine::java(app_signals, "AggregateWrapper", node);

    /*
    * step 6, shuffle application state by job id, together with metric signals

    // join those that only have job id
    DataStream<FlinkJobInfo> flinkJobInfoStream = appIdStates.connect(jobIdSignalStream)
        .keyBy(State::getJobId, Signal::getJobId)
        .process(new StateSignalJoiner(stateTtlInDays))
        .setParallelism(parseInt(configuration.getJavaString("keyby.parallelism")));
    */
    auto job_signals = engine::shuffle(
        app_state, "jobid", [](size_t hash, int rank, int workers) { return hash % workers; }, false, node);

    auto outputs = cp::DeclarationToBatches(std::move(job_signals)).ValueOrDie();

    size_t global_row_count = 0;
    MPI_Allreduce(&local_row_count, &global_row_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    // label
    float throughput = global_row_count / (MPI_Wtime() - start);
    if (node->rank == 0) {
      std::cout << "iteration pull " << throughput << " @ qps" << std::endl;
    }
  }
  return terminal_signal;
}
