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

using namespace surfingdb::table::schema;
using surfingdb::meta::node;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std::chrono;

/** run this program with
 * mpirun -np 12 ./Test
 * @return
 */
int main(int argc, char** argv) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);

  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);

  RowSchema r;

  SchemaUtils::addElements(r, "timestamp", RowType::LONG, 1);
  SchemaUtils::addElements(r, "host", RowType::STRING, 1);
  SchemaUtils::addElements(r, "metricName", RowType::STRING, 1);
  SchemaUtils::addElements(r, "metricValues", RowType::DOUBLE, 2);
  SchemaUtils::addPairs(r, "meta", RowType::STRING, RowType::STRING, 16);
  std::shared_ptr<TableSchema> schema_ptr = std::make_shared<TableSchema>(r);

    

  return 0;
}