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

#ifndef SURFINGDB_DATAGEN_H
#define SURFINGDB_DATAGEN_H

#include <rdkafka.h>
#include <string>
#include <vector>
#include "meta/node.h"
#include "meta/schema.h"
#include "table/mtable.h"
#include "table/row.h"

namespace surfingdb {
namespace connector {
using namespace surfingdb::meta;
using namespace surfingdb::table;

/**
 * thin kafka client wrapper
 * https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html
 */
class DataGenConnector {
public:
  DataGenConnector(std::shared_ptr<node>);

  std::shared_ptr<mtable>
    consume_batch(size_t max_batch_size, int timeout, std::shared_ptr<TableSchema> schema_ptr,
                  std::function<std::shared_ptr<RowBuffer>(const char* payload,
                                                           std::shared_ptr<TableSchema> schema_ptr)> deser);

private:
  std::shared_ptr<node> node_ptr;
};
} // namespace connector

} // namespace surfingdb
#endif // SURFINGDB_DATAGEN_H
