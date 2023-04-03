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
  SchemaUtils::initField(r, "topic", RowType::STRING, 64);
  SchemaUtils::initField(r, "payload", RowType::STRING, MAX_STR_LEN);
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief initial constructors
   * node -> single executor binding to MPI rank, number of node determined by mpi processes
   * mschema -> row based MPI friendly schema defined to encode/decode table/row in O(1) time
   * con -> data connector ingess running on a number of nodes micro batching data pullers
   */
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  auto con = DataGenConnector(node, "source", BATCH_SIZE, 10000, schema_ptr);

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
    /**
     * @brief import pyarrow
     */
    // arrow::py::import_pyarrow();c10d::ProcessGroupMPI::createProcessGroupMPI();

    const size_t intial_row_count = BATCH_SIZE;
    size_t total_row_count = intial_row_count;
    double start = MPI_Wtime();

    auto arrow_t2 = con.consume_batch([&schema_ptr](const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
      PValue v1, v2;
      Value placeholder;
      v1.string_val = "metric";
      v2.string_val = "{\"timestamp\": 1680480831430, \"host\": \"xenon-prod-001-20220810-dpp-worker-prod-0a02070a\", \"metricName\": \"flink.operator._t_host.xenon-prod-001-20220810-dpp-worker-prod-0a02070a_ec2_pin220_com._t_tm_id.container_1661534748548_105064_01_000025._t_job_id.ebcdb5d6a8e6a51abc2e9c4d34f9508b._t_job_name.K8sAuditStreamExample-prod._t_operator_id.7df19f87deec5680128845fd9a6ca18d._t_operator_name.Flat Map._t_subtask_index.10._t_objectName.tcp--evaluationjob--yzjze390-master-0.k8s_event_object\", \"metricValue\": \"1\" }";
      utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder);
      utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
    });
    CHECK(arrow_t2->num_rows() == BATCH_SIZE);

    auto t3 = processors::java(arrow_t2, "CleanupWrapper", node);
    std::map<std::string, uint64_t> units = utils::toUnits(t3);
    auto schema = utils::fromArrow(t3->schema(), units);
    auto mtable = utils::fromArrow(t3, units, node);
    auto t5 = processors::shuffle(mtable, schema->fields.at(0), [](size_t key, int rank, int world) {
      return key % world;
    });
    t5->verifyShuffle(schema->fields.at(0), [](size_t key, int rank, int world) {
      return key % world;
    });
    auto at5 = utils::toArrow(t5);
    auto t6 = processors::java(at5, "AggregateWrapper", node);
    units = utils::toUnits(t6);
    auto t7 = utils::fromArrow(t6, units, node);
    if(t7->row_count > 0) {
      std::cout << t7->row_count;
    }
    if (node->rank == 0) std::cout << "iteration" << BATCH_SIZE * node->world << std::endl;
    /*
    auto source = engine::source(arrow_t2);
    cp::Expression filter_expr = cp::less(cp::field_ref("timestamp"), cp::literal(3));
    auto filter_ = engine::filter(source, filter_expr);
    auto batches = cp::DeclarationToBatches(std::move(filter_)).ValueOrDie();
    */
  }
  return terminal_signal;
}
