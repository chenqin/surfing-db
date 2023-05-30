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
#include "datagen.h"

#include <ctype.h>
#include <glog/logging.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include <chrono>
#include <csignal>
#include <vector>

#include "table/utils.h"

namespace matcha {
namespace connector {
DataGenConnector::DataGenConnector(const std::shared_ptr<node> node_ptr,
                                   std::string connector_name,
                                   size_t max_batch_size, int timeout,
                                   std::shared_ptr<mschema> schema_ptr)
    : Connector(node_ptr, connector_name, max_batch_size, timeout, schema_ptr) {
}

std::shared_ptr<mtable> DataGenConnector::consume_batch(
    std::function<std::shared_ptr<mrow>(const char* payload,
                                        const mschema& schema)>
        deser) {
  /**
   * @brief if node runs data polling set to max_batch, otherwise skip
   *
   */
  auto t = std::make_shared<mtable>(node_ptr, schema_ptr,
                                    max_batch_size * schema_ptr->rowSize());
  auto start = MPI_Wtime();
  int total = 0;
  while ((MPI_Wtime() - start) * 1000 < timeout && total++ < max_batch_size) {
    auto b = deser(nullptr, *schema_ptr.get());
    if (b != nullptr) {
      t->appendRow(*b.get());
    }
  }
  return t;
}

std::shared_ptr<arrow::RecordBatch> DataGenConnector::consume_batch(
    std::function<
        size_t(const void* payload, size_t len,
             std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)>
        deser) {
  // auto schema = utils::toArrow(this->schema_ptr);
  auto start = MPI_Wtime();
  int total = 0;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  for (auto i = 0; i < schema_ptr->fields.size(); i++) {
    auto type = schema_ptr->fields.at(i).type;
    if (type == RowType::LIST) {
      auto keytype = schema_ptr->fields.at(i).list_type;
      builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, utils::getBuilder(keytype)));
      continue;
    }
    if (type == RowType::MAP) {
      auto keytype = schema_ptr->fields.at(i).map_key_type;
      auto valuetype = schema_ptr->fields.at(i).map_value_type;
      builders.push_back(std::make_shared<arrow::MapBuilder>(
          pool, utils::getBuilder(keytype), utils::getBuilder(valuetype)));
      continue;
    }
    builders.push_back(utils::getBuilder(type));
  }

  while ((MPI_Wtime() - start) * 1000 < timeout && total < max_batch_size) {
    stringstream ss;
    ss << node_ptr->rank;
    string str = ss.str();
    deser(str.c_str(), str.length(), builders);
    total++;
  }
  for (auto b : builders) {
    std::shared_ptr<arrow::Array> _array;
    b->Finish(&_array);
    arrays.push_back(_array);
  }
  return arrow::RecordBatch::Make(utils::toArrow(schema_ptr), total, arrays);
}
}  // namespace connector
}  // namespace matcha