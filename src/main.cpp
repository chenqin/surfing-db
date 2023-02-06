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
  SchemaUtils::appendElements(r, "metricValues", RowType::DOUBLE, 1);
  std::shared_ptr<TableSchema> schema_ptr = std::make_shared<TableSchema>(r);

  int rows = 1500;
  size_t total = 0;
  srand(std::time(nullptr));

  auto start = MPI_Wtime();
  auto results_ptr = std::make_shared<std::unordered_map<Value, std::shared_ptr<RowBuffer>, ValueHasher>>();
  std::string kafka_topic = "xenon_metrics_prod";
  std::string brokers = "datakafka08001:9092,datakafka08002:9092,datakafka08003:9092";
  std::string group_id = "surfing.test";

  // threaded ingest - map - shuffle - reduce

  auto consumer = KafkaConnector(node, kafka_topic, brokers, group_id);
  auto t1 = std::make_shared<mtable>(node, schema_ptr, rows * schema_ptr->rowSize());

  // simulate a delay to decode and handle kafka batch
  std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));

  // kafka consumer
  auto arrow_table = consumer.consume_batch(rows, 100, schema_ptr, [=](const char* payload, std::shared_ptr<surfingdb::meta::TableSchema> schema_ptr) -> std::shared_ptr<RowBuffer> {
    auto r = std::make_shared<RowBuffer>(schema_ptr);
    rapidjson::Document document;
    rapidjson::ParseResult ok = document.Parse((const char*)payload);
    if (ok) {
      for (auto f : schema_ptr->fields) {
        Value v;
        if (f.type == RowType::LONG) {
          v.p_val.long_val = document[f.name.c_str()].GetInt64();
          r->write(f, v);
        } else if (f.type == RowType::STRING) {
          CHECK(document[f.name.c_str()].GetStringLength() < MAX_STR_LEN);
          v.p_val.string_val = std::string(document[f.name.c_str()].GetString());
        } else if (f.type == RowType::DOUBLE) {
          std::string val = std::string(document[f.name.c_str()].GetString());
          try {
            v.p_val.double_val = (float)std::stod(val);
          } catch (std::exception& e) {
            v.p_val.double_val = 0;
          }
        }
        r->write(f, v);
      }
      return r;
    } else {
      LOG(INFO) << "invalid data";
      return nullptr;
    }
  });
  /**
   * @brief use arrow filter expressions
   *  
   */
  /**
   * shuffle if needed
  */
}
