/*
 * Copyright Chen Qin on 12/30/22.
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
#include <csignal>
#include <fmt/core.h>
#include <glog/logging.h>
#include <iostream>

#include "connector/datagen.h"
#include "meta/node.h"
#include "table/processors.h"
#include "table/utils.h"

#define FLUSH_DIR "/tmp/"
#define BATCH_SIZE 1

using namespace surfingdb::meta;
using namespace surfingdb::table::schema;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std;

volatile std::sig_atomic_t terminal_signal;
void signal_handler(int signal) {
  std::cout << "user exit program";
  terminal_signal = signal;
}

/** run this program with
 * mpirun -np 12 ./Test
 * @return
 */
int main(int argc, char** argv) {

  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);
  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::initField(r, "topic", RowType::STRING, 64);
  SchemaUtils::initField(r, "payload", RowType::STRING, 1024);
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief initial constructors
   * node -> single executor binding to MPI rank, number of node determined by mpi processes
   * mschema -> row based MPI friendly schema defined to encode/decode table/row in O(1) time
   * con -> data connector ingess running on a number of nodes micro batching data pullers
   */
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  auto con = DataGenConnector(node, "source", BATCH_SIZE, 10, schema_ptr);
  con.setDeser([&schema_ptr](const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
    PValue v1, v2;
    Value placeholder;
    stringstream ss;
    v1.string_val = string(payload);
    v2.string_val = "{\"timestamp\": 1680480831430, \"host\": \"xenon-prod-001-20220810-dpp-worker-prod-0a02070a\", \"metricName\": \"flink.operator._t_host.xenon-prod-001-20220810-dpp-worker-prod-0a02070a_ec2_pin220_com._t_tm_id.container_1661534748548_105064_01_000025._t_job_id.ebcdb5d6a8e6a51abc2e9c4d34f9508b._t_job_name.K8sAuditStreamExample-prod._t_operator_id.7df19f87deec5680128845fd9a6ca18d._t_operator_name.Flat Map._t_subtask_index.10._t_objectName.tcp--evaluationjob--yzjze390-master-0.k8s_event_object\", \"metricValue\": \"1\" }";
    utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder);
    utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
  });
  auto partitioner = [](size_t key, int rank, int world) { return key % world; };
  auto t1 = std::async(std::launch::async, [&con, &terminal_signal] {
    con.run(terminal_signal);
  });

  std::signal(SIGTERM | SIGINT, signal_handler);
  int itr = 0;
  while (terminal_signal == 0) {
    auto records = con.poll_once();
    auto signals = processors::java_x(records, "CleanupWrapper", node->env);
    auto app_signals = processors::shuffle_x(signals, "appid", partitioner, false, node->rank, node->world);
    auto app_state = processors::java_x(app_signals, "AggregateWrapper", node->env);
    auto job_signals = processors::shuffle_x(app_state, "jobid", partitioner, false, node->rank, node->world);
    auto job_info = processors::java_x(job_signals, "JobInfoWrapper", node->env);
  }
  t1.wait();
  return terminal_signal;
}
