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
#include <iostream>
#include <fstream>
#include <string>
#include <future>
#include <glog/logging.h>
#include <iostream>
#include <experimental/random>
#include <omp.h>
#include <rapidjson/document.h>
#include "connector/kafka.h"
#include "meta/node.h"
#include "engine/engine.h"
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

std::string serversettobrokers(std::string serverset) {
  std::string line;
  std::string brokers = "";
  int count = 0;
  int start = std::experimental::randint(8, 20);
  std::ifstream myfile(serverset.c_str());
  if (myfile.is_open())
  {
    while ( getline (myfile,line) && count < 4)
    {
      //std::cout << line << '\n';
      if(start-- < 0){
        brokers += line + ",";
        count++;
      }
    }
    myfile.close();
  }

  else std::cout << "Unable to open file"; 
  return brokers.substr(0, brokers.length() - 1);
}

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
  //auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  /**
   * @brief define topic schema
   *
   */
  RowSchema r;
  SchemaUtils::initField(r, "topic", RowType::STRING, 64);
  SchemaUtils::initField(r, "payload", RowType::STRING, MAX_STR_LEN);
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);
  std::map<std::string, uint64_t> units = { { "topic", 64 }, { "payload", MAX_STR_LEN }};

  /**
   * features
   */
  int batch = 200000;
  int interval = 300;
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
    node, "kafka-source", batch, interval, schema_ptr, metric_deser,
    {"xenon_metrics_prod"}, serversettobrokers("/var/serverset/discovery.datakafka08.prod"), group_id, false);

  auto metrics_staging = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr, metric_deser,
     {"xenon_metrics_staging"}, serversettobrokers("/var/serverset/discovery.datakafka08.prod"), group_id, false);

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
    node, "kafka-source", batch, interval, schema_ptr, metric_log_deser,
     {"xenon-logs-staging"}, serversettobrokers("/var/serverset/discovery.metricskafka07.prod"), group_id, false);

  auto metrics_log_prod = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr, metric_log_deser,
     {"xenon-logs-prod"}, serversettobrokers("/var/serverset/discovery.metricskafka07.prod"), group_id, false);

  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    // kafka consumer
    
    auto t1 = std::async(std::launch::async, [&metrics_prod] { return metrics_prod.consume_batch();});
    auto t2 = std::async(std::launch::async, [&metrics_staging] { return metrics_staging.consume_batch();});
    auto t3 = std::async(std::launch::async, [&metrics_log_staging] { return metrics_log_staging.consume_batch();});
    auto t4 = std::async(std::launch::async, [&metrics_log_prod] { return metrics_log_prod.consume_batch();});
    std::vector<std::shared_ptr<mtable>> inputs;
    inputs.push_back(t1.get());
    inputs.push_back(t2.get());
    inputs.push_back(t3.get());
    inputs.push_back(t4.get());
    size_t local_row_count = 0;
    for(auto& t : inputs) {
      auto tjava = processors::java(t, "Bridge");

      /*
      * assign data gather from rest of workers to gpu backed worker
      */
      auto partitioner = [](size_t key, int rank, int world) {
        return key % world;
      };

      //auto tshuffle = processors::shuffle(tjava, schema_ptr->fields.at(0), partitioner);
      //t3->verifyShuffle(schema_ptr->fields.at(2), partitioner);

      local_row_count += t->row_count;
    }
    size_t global_row_count = 0;
    MPI_Allreduce(&local_row_count, &global_row_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    // label
    float throughput = global_row_count / (MPI_Wtime() - start);
    //processors::mnist(pg, t4);
    if (node->rank == 0) {
      std::cout << "iteration pull " << throughput << " @ qps" << std::endl;
    }
  }
  /**
   * @brief use arrow filter expressions
   *
   */
  /**
   * shuffle if needed
   */
  return terminal_signal;
}
