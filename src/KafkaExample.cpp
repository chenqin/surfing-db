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
  int batch = 10000;
  int interval = 100;
  int world = node->world;

  size_t total = 0;
  srand(std::time(nullptr));

  auto start = MPI_Wtime();
  std::string group_id = "cqin-test";

  std::signal(SIGTERM | SIGINT, signal_handler);

  auto metrics_prod = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr,
    { "xenon_metrics_prod" }, "/var/serverset/discovery.datakafka08.prod", group_id, false);
  metrics_prod.setDeser([&schema_ptr](const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
    PValue v1, v2;
    Value placeholder;
    v1.string_val = "metric";
    v2.string_val = std::string(payload);
    utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder);
    utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
  });

  auto metrics_log_prod = KafkaConnector(
    node, "kafka-source", batch / 10, interval, schema_ptr,
    { "xenon-logs-prod" }, "/var/serverset/discovery.metricskafka07.prod", group_id, false);
  metrics_log_prod.setDeser([&schema_ptr](const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
    PValue v1, v2;
    Value placeholder;
    v1.string_val = "log";
    v2.string_val = std::string(payload);
    utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder);
    utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
  });

  auto t1 = std::async(std::launch::async, [&metrics_prod, &terminal_signal] {
    metrics_prod.run(terminal_signal);
  });
  auto t2 = std::async(std::launch::async, [&metrics_log_prod, &terminal_signal] {
    metrics_log_prod.run(terminal_signal);
  });

  auto partitioner = [](size_t key, int rank, int world) { return key % world; };

  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    long local_row_count = 0;
    // kafka consumer

    auto metric_table = metrics_prod.poll_once();
    for(auto&t : metric_table ){
      local_row_count += t->num_rows();
    }
    // local_row_count += metric_table->num_rows();
    auto log_table = metrics_log_prod.poll_once();
    // local_row_count += log_table->num_rows();
     for(auto&t : log_table ){
      local_row_count += t->num_rows();
    }
    
    auto signal_metric = processors::java_x(metric_table, "CleanupWrapper", node);
    auto signal_log = processors::java_x(log_table, "CleanupWrapper", node);
    
    signal_metric.insert(signal_metric.end(), signal_log.begin(), signal_log.end());

    auto app_signals = processors::shuffle_x(signal_metric, "appid", partitioner, false, node);
    auto app_state = processors::java(app_signals, "AggregateWrapper", node);
    auto job_signals = processors::shuffle_x({ app_state }, "jobid", partitioner, true, node);
    auto job_info = processors::java(job_signals, "JobInfoWrapper", node);

    size_t global_row_count = 0;
    MPI_Allreduce(&local_row_count, &global_row_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    // label
    float throughput = global_row_count / (MPI_Wtime() - start);
    if (node->rank == 0) {
      std::cout << "iteration pull " << throughput << " @ qps" << std::endl;
    }
  }
  t1.wait();
  t2.wait();
  return terminal_signal;
}
