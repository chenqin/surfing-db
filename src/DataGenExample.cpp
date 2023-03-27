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
#include <future>
#include <glog/logging.h>
#include <iostream>
#include <jni.h>
#include <omp.h>
#include <rapidjson/document.h>
#include "connector/datagen.h"
#include "engine/engine.h"
#include "meta/node.h"
#include "table/processors.h"

#define FLUSH_DIR "/tmp/"
#define BATCH_SIZE 2250

using namespace surfingdb::meta;
using namespace surfingdb::table::schema;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace surfingdb::engine;
using namespace std;

namespace cp = ::arrow::compute;

/**
 * https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
 */
std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

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
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, 1024);
  SchemaUtils::initListField(r, "metricValues", RowType::DOUBLE, 2, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(r, "meta", RowType::STRING, RowType::STRING, 1, 32, 64);
  std::map<std::string, uint64_t> units = { {"host", 1024}, { "metricValues", 2 }, { "meta", 1 } };
  /**
   * @brief initial constructors
   * node -> single executor binding to MPI rank, number of node determined by mpi processes
   * mschema -> row based MPI friendly schema defined to encode/decode table/row in O(1) time
   * con -> data connector ingess running on a number of nodes micro batching data pullers
   */
  auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);

  const auto schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief define a data gen source
   *
   */
  auto deser = [](const char* payload, const mschema& out) {
    auto row = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value p;

    p.p_val.long_val = 1;
    row->write(out.fields.at(0), p);

    p.p_val.string_val = "hello_host";
    row->write(out.fields.at(1), p);

    p.p_val.string_val = random_string(16);
    row->write(out.fields.at(2), p);

    p.p_val.double_val = 0.1;
    std::vector<PValue> lval;
    lval.push_back(p.p_val);
    lval.push_back(p.p_val);
    p.list_value = lval;
    row->write(out.fields.at(3), p);
    PValue key;
    PValue value;
    key.string_val = random_string(1024 - 1);
    value.string_val = random_string(1024 - 1);
    std::pair<PValue, PValue> pair;
    pair.first = key;
    pair.second = value;
    p.map_value.insert(pair);
    row->write(out.fields.at(4), p);
    return row;
  };
  auto con = DataGenConnector(node, "source", BATCH_SIZE, 10000, schema_ptr, deser);

  std::signal(SIGTERM | SIGINT, signal_handler);

  /**
   * @brief
   * show case consumer send data async to ranks not pulling data
   * so that while other workers working on shuffle or post shuffle stages
   * consumer ranks can async send data to other ranks
   * jump to next iteration and get next batch ready
   */
  auto partitioner = [](size_t key, int rank, int world) {
    int base = world % 2 == 0 ? world - 1 : world;
    int dest = key % world;
    /**
     * @brief avoid use GPU rank in preprocessing
     *
     */
    return dest == 0 ? dest + 1 : dest;
  };

  while (terminal_signal == 0) {
    if (node->rank == 0) std::cout << "iteration" << std::endl;
    /**
     * @brief import pyarrow
     */
    // arrow::py::import_pyarrow();c10d::ProcessGroupMPI::createProcessGroupMPI();

    const size_t intial_row_count = BATCH_SIZE;
    size_t total_row_count = intial_row_count;
    double start = MPI_Wtime();
    auto t1 = con.consume_batch();
    //std::cout << node->rank << " " << t1->row_count << std::endl;
    std::shared_ptr<mtable> shuffle_in = t1;

    auto source = engine::source(t1);
    cp::Expression filter_expr = cp::less(cp::field_ref("timestamp"), cp::literal(3));
    auto filter_ = engine::filter(source, filter_expr);
    auto batches = cp::DeclarationToBatches(std::move(filter_)).ValueOrDie();
    
    for (int i = 0; i < batches.size(); i++) {
      auto t2 = utils::fromArrow(batches.at(i), units, node);
      auto schema_1 = utils::fromArrow(batches.at(i)->schema(), units);
      /**
       * @brief transform data into shuffle schema
       * 
       */
      shuffle_in = processors::map(t2, schema_ptr, [&](mrow& in, mrow& out, const mschema& out_schema) {
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

    auto t5 = processors::shuffle(shuffle_in, schema_ptr->fields.at(2), partitioner);
    t5->verifyShuffle(schema_ptr->fields.at(2), partitioner);
    std::map<std::string, uint64_t> units;
    auto t41 = processors::java(t5, "Bridge", units);
    processors::mnist(pg, t41);
  }
  return terminal_signal;
}
