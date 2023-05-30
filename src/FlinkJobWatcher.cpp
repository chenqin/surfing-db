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

#include <glog/logging.h>

#include <chrono>
#include <future>
#include <iostream>
#include <string>

#include "connector/kafka.h"
#include "meta/node.h"
#include "table/processors.h"
#include "table/utils.h"

using namespace matcha::table::schema;
using matcha::meta::node;
using namespace matcha::table;
using namespace matcha::connector;
using namespace std::chrono;
namespace cp = ::arrow::compute;

volatile std::sig_atomic_t terminal_signal;
void signal_handler(int signal) {
  std::cout << "user exit program";
  terminal_signal = signal;
}

int main(int argc, char* argv[]) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);
  CHECK_GT(argc, 1);
  std::string jar(argv[1]);
  //  create node of cluster
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<matcha::meta::node>(&argc, &argv, jar);
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
  int batch = 500000;
  int interval = 300;
  int world = node->world;

  size_t total = 0;
  srand(std::time(nullptr));

  auto start = MPI_Wtime();
  CHECK_GT(argc, 2);
  std::string group_id(argv[2]);

  std::signal(SIGTERM | SIGINT, signal_handler);

  auto metrics_prod = KafkaConnector(
      node, "kafka-source", batch, interval, schema_ptr, {"xenon_metrics_prod"},
      "/var/serverset/discovery.datakafka08.prod", group_id, false);
  metrics_prod.setDeser(
      [&schema_ptr](
          const void* payload, size_t len,
          std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        PValue v1, v2;
        Value placeholder;
        v1.string_val = "metric";
        v2.string_val = std::string((char*) payload);
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1,
                      placeholder);
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2,
                      placeholder);
        return 1;
      });

  auto metrics_prod_pii = KafkaConnector(
      node, "kafka-source", batch, interval, schema_ptr,
      {"xenon_metrics_prod_pii"}, "/var/serverset/discovery.datakafka08.prod",
      group_id, false);
  metrics_prod_pii.setDeser(
      [&schema_ptr](
          const void* payload, size_t len,
          std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        PValue v1, v2;
        Value placeholder;
        v1.string_val = "metric";
        v2.string_val = std::string((char*) payload);
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1,
                      placeholder);
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2,
                      placeholder);
        return 1;
      });

  auto log_prod = KafkaConnector(
      node, "kafka-source", batch, interval, schema_ptr, {"xenon-logs-prod"},
      "/var/serverset/discovery.metricskafka07.prod", group_id, false);
  log_prod.setDeser(
      [&schema_ptr](
          const void* payload, size_t len,
          std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        PValue v1, v2;
        Value placeholder;
        v1.string_val = "log";
        v2.string_val = std::string((char*) payload);
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1,
                      placeholder);
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2,
                      placeholder);
        return 1;
      });
  /*
  auto log_prod_pii = KafkaConnector(
      node, "kafka-source", batch, interval, schema_ptr,
  {"xenon-logs-pii-prod"},
      "/var/serverset/discovery.metricskafka07.prod", group_id, true);
  log_prod_pii.setDeser(
      [&schema_ptr](
          const char* payload,
          std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        PValue v1, v2;
        Value placeholder;
        v1.string_val = "log";
        v2.string_val = std::string(payload);
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1,
                      placeholder);
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2,
                      placeholder);
      });
  */
  auto t1 = std::async(std::launch::async, [&metrics_prod, &terminal_signal] {
    metrics_prod.run(terminal_signal);
  });
  auto t3 =
      std::async(std::launch::async, [&metrics_prod_pii, &terminal_signal] {
        metrics_prod_pii.run(terminal_signal);
      });

  auto t2 = std::async(std::launch::async, [&log_prod, &terminal_signal] {
    log_prod.run(terminal_signal);
  });
  /*
  auto t4 = std::async(std::launch::async, [&log_prod_pii, &terminal_signal] {
    log_prod_pii.run(terminal_signal);
  });
  */
  auto partitioner = [](size_t key, int rank, int world) {
    return key % world;
  };

  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    long local_row_count = 0;
    // kafka consumer

    auto m1 = metrics_prod.poll_once(utils::toArrow(schema_ptr));
    local_row_count += m1->num_rows();

    auto m2 = metrics_prod_pii.poll_once(utils::toArrow(schema_ptr));
    local_row_count += m2->num_rows();

    auto m3 = log_prod.poll_once(utils::toArrow(schema_ptr));
    local_row_count += m3->num_rows();
    // auto m4 = log_prod_pii.poll_once();
    // for (auto& t : m4) local_row_count += t->num_rows();
/*
    m1 = utils::merge({m1, m2}, m1->schema());
    // m3.insert(m3.end(), m4.begin(), m4.end());

    auto signal_metric =
        processors::jni({m1}, "com/pinterest/drsquirrel/Cleanup", node->env, node->rank);
    auto signal_log =
        processors::jni({m3}, "com/pinterest/drsquirrel/Cleanup", node->env, node->rank);

    signal_metric.insert(signal_metric.end(), signal_log.begin(),
                         signal_log.end());
    auto app_signals = processors::shuffle(signal_metric, "appid", partitioner,
                                           true, node->rank, node->world);
    auto app_state = processors::jni(
        app_signals, "com/pinterest/drsquirrel/Aggregate", node->env, node->rank);

    auto job_signals = processors::shuffle(app_state, "jobid", partitioner,
                                           true, node->rank, node->world);
    auto job_info = processors::jni(
        job_signals, "com/pinterest/drsquirrel/JobInfo", node->env, node->rank);
*/
    size_t global_row_count = 0;
    MPI_Allreduce(&local_row_count, &global_row_count, 1, MPI_UNSIGNED_LONG,
                  MPI_SUM, MPI_COMM_WORLD);

    // label
    float throughput = global_row_count / (MPI_Wtime() - start);
    if (node->rank == 0)
      LOG(INFO) << throughput << " qps " << (MPI_Wtime() - start) << " seconds "
                << std::endl;
    utils::jvmGC(node->env);
    std::this_thread::sleep_for(60s);
  }
  t1.wait();
  t2.wait();
  t3.wait();
  return terminal_signal;
}
