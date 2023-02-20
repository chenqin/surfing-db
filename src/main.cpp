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
#include "connector/kafka.h"
#include "meta/node.h"
#include "table/processors.h"

#define FLUSH_DIR "/tmp/"

using namespace surfingdb::table::schema;
using surfingdb::meta::node;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std::chrono;

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
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  /**
   * @brief define topic schema
   *
   */
  RowSchema r;
  SchemaUtils::appendElements(r, "timestamp", RowType::LONG, 1);
  SchemaUtils::appendElements(r, "host", RowType::STRING, 1);
  SchemaUtils::appendElements(r, "metricName", RowType::STRING, 1);
  SchemaUtils::appendElements(r, "metricValue", RowType::DOUBLE, 1);
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);

  int rows = 300000;
  size_t total = 0;
  srand(std::time(nullptr));

  auto start = MPI_Wtime();
  std::string kafka_topic = "xenon_metrics_prod";
  /**
   * read from /var/serverset/datakafka08
   */
  std::string brokers = "10.1.145.151:9092,10.1.145.239:9092,10.1.147.235:9092,10.1.148.60:9092";
  std::string group_id = "cqin-test";

  std::signal(SIGTERM|SIGINT, signal_handler);

  auto consumer = KafkaConnector(node, kafka_topic, brokers, group_id);
  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    // kafka consumer
    auto t1 = consumer.consume_batch(rows, 1000, schema_ptr, [](const char* payload, const mschema& out) {
      auto r = std::make_shared<mrow>(std::make_shared<mschema>(out));
      rapidjson::Document document;
      bool err = document.Parse((const char*)payload).HasParseError();
      if (!err && document.HasMember("timestamp")) {
        Value v[4];
        v[0].p_val.long_val = document["timestamp"].GetInt64();
        v[1].p_val.string_val = document["host"].GetString();
        v[2].p_val.string_val = document["metricName"].GetString();
        v[3].p_val.double_val = std::atof(document["metricValue"].GetString());
        for (int i = 0; i < 4; i++) {
          r->write(out.fields.at(i), v[i]);
        }
        return r;
      } else {
        LOG(INFO) << "invalid data";
        std::shared_ptr<mrow> p2(nullptr);
        return p2;
      }
    });
    auto partitioner = [](size_t key, int rank, int world) {
      int base = world % 2 == 0 ? world - 1 : world;
      int dest = key % base;
      CHECK_GE(dest, 0);
      CHECK_LT(dest, world);
      return dest;
    };
    auto t3 = processors::shuffle(t1, schema_ptr->fields.at(2), partitioner);
    t3->verifyShuffle(schema_ptr->fields.at(2), partitioner);

    processors::compute(t3, [](std::shared_ptr<mtable> m) {
      return arrow::compute::Sum({ m->getRecordBatch()->GetColumnByName("metricValue") });
    });
    auto end = MPI_Wtime();
    size_t local_row_count = t1->row_count;
    size_t global_row_count = 0;
    MPI_Allreduce(&local_row_count, &global_row_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
    
    if (node->rank == 0) {
      std::cout << "iteration process " << global_row_count << " rows in " << end - start << " seconds" << std::endl;
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
