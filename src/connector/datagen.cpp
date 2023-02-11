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
#include <chrono>
#include <csignal>
#include <ctype.h>
#include <glog/logging.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <vector>

namespace surfingdb {
namespace connector {

DataGenConnector::DataGenConnector(const std::shared_ptr<node> node_ptr) {
  this->node_ptr = node_ptr;
}

std::shared_ptr<mtable> DataGenConnector::consume_batch(size_t max_batch_size, int timeout, std::shared_ptr<TableSchema> schema_ptr, std::function<std::shared_ptr<RowBuffer>(const char*, const TableSchema&)> deser) {
  /**
   * @brief if node runs data polling set to max_batch, otherwise skip
   *
   */
  auto batch_size = node_ptr->getIsSubscriber() != 1 ? 0 : max_batch_size;
  auto t = std::make_shared<mtable>(node_ptr, schema_ptr, batch_size * schema_ptr->rowSize());
  auto start = MPI_Wtime();
  int total = 0;
  while ((MPI_Wtime() - start) * 1000 < timeout && total++ < batch_size) {
    auto b = deser(nullptr, *schema_ptr.get());
    if (b != nullptr) {
      t->appendRow(*b.get());
    }
  }
  return t;
}
} // namespace connector
} // namespace surfingdb