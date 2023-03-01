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
#include <arrow/device.h>
#include <arrow/gpu/cuda_api.h>
#include <arrow/gpu/cuda_context.h>
#include <csignal>
#include <fmt/core.h>
#include <future>
#include <glog/logging.h>
#include <iostream>
#include <jni.h>
#include <omp.h>
#include <rapidjson/document.h>
#include "connector/datagen.h"
#include "meta/node.h"
#include "table/processors.h"

#define FLUSH_DIR "/tmp/"
#define BATCH_SIZE 2250

using namespace surfingdb::meta;
using namespace surfingdb::table::schema;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std;
using namespace arrow::cuda;
using namespace arrow;

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
  SchemaUtils::appendElements(r, "timestamp", RowType::LONG, 1);
  SchemaUtils::appendElements(r, "host", RowType::STRING, 1);
  SchemaUtils::appendElements(r, "metricName", RowType::STRING, 1);
  // min, max of metricValue
  SchemaUtils::appendElements(r, "metricValues", RowType::DOUBLE, 2);
  // user defined meta data pair
  SchemaUtils::appendPairs(r, "meta", RowType::STRING, RowType::STRING, 1);

  /**
   * @brief initial constructors
   * node -> single executor binding to MPI rank, number of node determined by mpi processes
   * mschema -> row based MPI friendly schema defined to encode/decode table/row in O(1) time
   * con -> data connector ingess running on a number of nodes micro batching data pullers
   */
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  const auto ptr = std::make_shared<mschema>(r);
  const auto con = std::make_unique<DataGenConnector>(node);

  std::signal(SIGTERM | SIGINT, signal_handler);

  /**
   * @brief
   * show case consumer send data async to ranks not pulling data
   * so that while other workers working on shuffle or post shuffle stages
   * consumer ranks can async send data to other ranks
   * jump to next iteration and get next batch ready
   */
  bool produce = node->rank % 2 == 0;
  node->setissubscriber(&produce);
  auto partitioner = [](size_t key, int rank, int world) {
    int base = world % 2 == 0 ? world - 1 : world;
    int dest = key % base;
    /**
     * @brief dest is subscriber to data ingestion
     *
     */
    if (dest % 2 == 0) {
      if (dest + 1 > world - 1) {
        dest = dest - 1;
      } else {
        dest = dest + 1;
      }
    }
    CHECK_GE(dest, 0);
    CHECK_LT(dest, world);
    return dest;
  };

  while (terminal_signal == 0) {
    /**
     * @brief import pyarrow
     */
    // arrow::py::import_pyarrow();

    const size_t intial_row_count = node->rank * BATCH_SIZE;
    size_t total_row_count = intial_row_count;
    double start = MPI_Wtime();
    // ingest, copy rows to local table memory with fixed offsets
    const auto t1 = con->consume_batch(intial_row_count, 1000, ptr, [](const char* payload, const mschema& out) {
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
      key.string_val = random_string(MAX_STR_LEN - 1);
      value.string_val = random_string(MAX_STR_LEN - 1);
      std::pair<PValue, PValue> pair;
      pair.first = key;
      pair.second = value;
      p.map_value.insert(pair);
      row->write(out.fields.at(4), p);
      return row;
    });

    /**
     * pass each row in mtable, if return true, add to new table with schema ptr
     * release t1 mtable in the end
     */
    auto t2 = processors::map(t1, ptr, [](mrow& in, mrow& out, const mschema& out_schema) {
      for (const auto& f : out_schema.fields) {
        Value v;
        in.read(f, v);
        out.write(f, v);
      }
      return true;
    });

    start = MPI_Wtime();
    auto t4 = processors::shuffle(t2, ptr->fields.at(2), partitioner);
    auto end = MPI_Wtime();
    // std::cout << " shuffle time = " << (end - start) << " rank = " << node->rank << " ingestor = " << node->getissubscriber() << std::endl;
    start = MPI_Wtime();
    /**
     * verify shuffle row placement to right worker (aka MPI rank)
     */
    t4->verifyShuffle(ptr->fields.at(2), partitioner);

    auto t41 = processors::java(t4, "Bridge");

    /**
     * @brief shuffle again with another field with same partitioner
     *
     */
    auto t5 = processors::shuffle(t4, ptr->fields.at(4), partitioner);
    t5->verifyShuffle(ptr->fields.at(4), partitioner);

    /**
     * @brief read data from java
     *
     */
    auto t51 = processors::java(t5, "MyBridge");

    CudaDeviceManager* manager_;
    std::shared_ptr<CudaDevice> device_;
    std::shared_ptr<CudaMemoryManager> mm_;
    std::shared_ptr<CudaContext> context_;
    std::shared_ptr<arrow::Device> cpu_device_;
    std::shared_ptr<MemoryManager> cpu_mm_;
    manager_ = CudaDeviceManager::Instance().ValueOrDie();
    device_ = manager_->GetDevice(0).ValueOrDie();
    context_ = device_->GetContext().ValueOrDie();
    mm_ = AsCudaMemoryManager(device_->default_memory_manager()).ValueOrDie();
    cpu_device_ = arrow::CPUDevice::Instance();
    cpu_mm_ = cpu_device_->default_memory_manager();


  }
  return terminal_signal;
}
