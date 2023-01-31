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
#define BATCH_SIZE 25600

using namespace surfingdb::table::schema;
using surfingdb::meta::node;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std;

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

  while (true) {

    // allocate large memory with fixed layout per column, row and fields
    auto t1 = std::make_shared<mtable>(node, ptr, BATCH_SIZE * ptr->rowSize());

    // ingest, copy rows to local table memory with fixed offsets
    for (int i = 0; i < BATCH_SIZE; i++) {
      auto row = std::make_unique<RowBuffer>(ptr);
      Value p;

      p.p_val.long_val = 1;
      row->write(ptr->fields.at(0), p);

      p.p_val.string_val = "hello_host";
      row->write(ptr->fields.at(1), p);

      p.p_val.string_val = std::to_string(node->rank);
      row->write(ptr->fields.at(2), p);

      p.p_val.double_val = 0.1;
      std::vector<PValue> lval;
      lval.push_back(p.p_val);
      lval.push_back(p.p_val);
      p.list_value = lval;
      row->write(ptr->fields.at(3), p);

      PValue key, value;
      key.string_val = "hello";
      value.string_val = "world";
      std::pair<PValue, PValue> pair;
      pair.first = key;
      pair.second = value;
      p.map_value.insert(pair);
      row->write(ptr->fields.at(4), p);

      t1->appendRow(*row.get());
    }

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
    auto t3 = processors::shuffle(t2, ptr->fields.at(2));
    /**
     * verify shuffle row placement to right worker (aka MPI rank)
     */
    t3->verifyShuffle(ptr->fields.at(2));
    /**
     * convert t3 mtable to columnar table
     * release t3 mtable vector memory
     */
    t3->toColumnar();
    if (node->rank == 0) {
      total += node->world * BATCH_SIZE;
      cout <<  "total rows processed " << total << endl;
    }
  }

  /*
  auto results_ptr = std::make_shared<std::unordered_map<Value, std::shared_ptr<RowBuffer>, ValueHasher>>();
  processors::reduce(t3, ptr->fields.at(2), results_ptr, ptr, [=](Value& key, std::vector<std::unique_ptr<RowBuffer>>& vals, std::shared_ptr<RowBuffer>& result) {
    Value v;
    result->read(ptr->fields.at(0), v);
    v.p_val.long_val += vals.size();

    result->write(ptr->fields.at(0), v);
  });
  */

  if (node->rank == 0) {
    cout << "all rank result size sum expect to equal number of ranks due to unqiue shuffle key (metric name)" << endl;
  }
  return 0;
}