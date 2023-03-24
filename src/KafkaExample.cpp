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
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  /**
   * @brief define topic schema
   *
   */
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initField(r, "metricValue", RowType::DOUBLE, sizeof(DOUBLE_TYPE));
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);
  std::map<std::string, uint64_t> units = { { "host", 64 }, { "metricName", MAX_STR_LEN }};

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

  auto deser = [](const char* payload, const mschema& out) {
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
    };

  auto metrics_prod = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr, deser,
    "xenon_metrics_prod", serversettobrokers("/var/serverset/discovery.datakafka08.prod"), group_id, false);

  auto metrics_staging = KafkaConnector(
    node, "kafka-source", batch, interval, schema_ptr, deser,
     "xenon_metrics_staging", serversettobrokers("/var/serverset/discovery.datakafka08.prod"), group_id, false);

  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    // kafka consumer
    auto t1 = metrics_prod.consume_batch();
    auto t11 = metrics_staging.consume_batch();
    std::shared_ptr<mtable> t_in;
    if(t1->row_count == 0 && t11->row_count == 0) {
      t_in = t1;
    } else if(t1->row_count == 0) {
      t_in = t11;
    } else {
      auto source = engine::source(t1);
      auto source_1 = engine::source(t1);
      auto uion_del = engine::union_op(source, source_1);
      auto batches = cp::DeclarationToBatches(std::move(uion_del)).ValueOrDie();

      for (int i = 0; i < batches.size(); i++) {
        auto t_111 = utils::fromArrow(batches.at(i), units, node);
        auto schema_1 = utils::fromArrow(batches.at(i)->schema(), units);
        /**
        * @brief transform data into shuffle schema
        * 
        */
        t_in = processors::map(t_111, schema_ptr, [&](mrow& in, mrow& out, const mschema& out_schema) {
          for (const auto& f : out_schema.fields) {
            Value v;
            /**
            * @brief read from old schema field
            * 
            */
            in.read(schema_1->getFieldByName(f.name), v);
            out.write(f, v);
          }
          return true;
        });
      }
    }
    /**
     * pass each row in mtable, if return true, add to new table with schema ptr
     * release t1 mtable in the end
     */
    auto t2 = processors::map(t_in, schema_ptr, [](mrow& in, mrow& out, const mschema& out_schema) {
      for (const auto& f : out_schema.fields) {
        Value v;
        in.read(f, v);
        out.write(f, v);
      }
      return true;
    });

    /*
     * assign data gather from rest of workers to gpu backed worker
     */
    auto partitioner = [](size_t key, int rank, int world) {
      return key % world;
    };

    auto t3 = processors::shuffle(t2, schema_ptr->fields.at(2), partitioner);
    t3->verifyShuffle(schema_ptr->fields.at(2), partitioner);

    auto t4 = processors::java(t3, "Bridge");

    auto end = MPI_Wtime();
    size_t local_row_count = t_in->row_count;
    size_t global_row_count = 0;
    MPI_Allreduce(&local_row_count, &global_row_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    // label
    float throughput = global_row_count / (end - start);

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
