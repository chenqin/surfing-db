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
#define BATCH_SIZE 22560

using namespace surfingdb::table::schema;
using surfingdb::meta::node;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std;

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

/** run this program with
 * mpirun -np 12 ./Test
 * @return
 */
int main(int argc, char** argv) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);

  // define where worker runs (as MPI rank)
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);

  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::appendElements(r, "timestamp", RowType::LONG, 1);
  SchemaUtils::appendElements(r, "host", RowType::STRING, 1);
  SchemaUtils::appendElements(r, "metricName", RowType::STRING, 1);
  // min, max of metricValue
  SchemaUtils::appendElements(r, "metricValues", RowType::DOUBLE, 2);
  // user defined meta data pair
  SchemaUtils::appendPairs(r, "meta", RowType::STRING, RowType::STRING, 1);

  // define max number of rows to ingest onetime per worker (total = np * batch_num)
  auto ptr = std::make_shared<TableSchema>(r);
  long total = 0;
  std::vector<std::shared_ptr<arrow::Table>> tables;

  /**
   * @brief import pyarrow
   */
  // arrow::py::import_pyarrow();

  size_t total_row_count = 0;
  const size_t intial_row_count = node->rank * BATCH_SIZE;
  // allocate large memory with fixed layout per column, row and fields
  auto t1 = std::make_shared<mtable>(node, ptr, intial_row_count * ptr->rowSize());

  double start = MPI_Wtime();
  // ingest, copy rows to local table memory with fixed offsets
  for (int i = 0; i < intial_row_count; i++) {
    total_row_count++;
    auto row = std::make_unique<RowBuffer>(ptr);
    Value p;

    p.p_val.long_val = 1;
    row->write(ptr->fields.at(0), p);

    p.p_val.string_val = "hello_host";
    row->write(ptr->fields.at(1), p);

    p.p_val.string_val = random_string(16);
    row->write(ptr->fields.at(2), p);

    p.p_val.double_val = 0.1;
    std::vector<PValue> lval;
    lval.push_back(p.p_val);
    lval.push_back(p.p_val);
    p.list_value = lval;
    row->write(ptr->fields.at(3), p);

    PValue key, value;
    key.string_val = random_string(16);
    value.string_val = random_string(16);
    std::pair<PValue, PValue> pair;
    pair.first = key;
    pair.second = value;
    p.map_value.insert(pair);
    row->write(ptr->fields.at(4), p);

    t1->appendRow(*row.get());
  }

  while (true) {
    /**
     * pass each row in mtable, if return true, add to new table with schema ptr
     * release t1 mtable in the end
     */
    auto t2 = processors::map(t1, ptr, [&](RowBuffer in, RowBuffer out) -> bool {
      for (auto f : ptr->fields) {
        Value v;
        in.read(f, v);
        out.write(f, v);
      }
      return true;
    });
    /**
     * MPI based shuffle based on hash value of a field
     * release t2 mtable in the end
     */
    auto start = MPI_Wtime();
    auto t3 = t2->placement_sort(ptr->fields.at(2));
    t2->release();
    auto end = MPI_Wtime();
    std::cout << "sort :" << (end - start);

    start = MPI_Wtime();
    auto t4 = processors::shuffle(t3, ptr->fields.at(2));
    end = MPI_Wtime();
    std::cout << " shuffle :" << (end - start) << " rank = " << node->rank << std::endl;
    start = MPI_Wtime();
    /**
     * verify shuffle row placement to right worker (aka MPI rank)
     */
    t4->verifyShuffle(ptr->fields.at(2));
    /**
     * convert t3 mtable to columnar table
     * release t3 mtable vector memory
     */
    processors::compute(t4, [](std::shared_ptr<mtable> m) {
        m->toColumnar();
        return arrow::compute::Sum({m->getArrowTable()->GetColumnByName("timestamp")});
    });
    // Write it using Datasets
    // auto pytable = arrow::py::wrap_table(t3->getArrowTable());
    /**
     * store post paritioned columnar table
     */
    // tables.push_back(t3->getArrowTable());
    size_t r_row_count = t4->getArrowTable()->num_rows();

    size_t g_init_total_count = 0, g_post_total_count = 0;
    MPI_Allreduce(&total_row_count, &g_init_total_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&r_row_count, &g_post_total_count, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
    if (node->rank == 0) {
      CHECK_EQ(g_init_total_count, g_post_total_count);
      cout << "total rows = " << g_init_total_count << endl;
    }
    t4->release();
  }
  return 0;
}