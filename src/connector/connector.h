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

#ifndef SURFINGDB_CONNECTOR_H
#define SURFINGDB_CONNECTOR_H

#include <string>
#include <vector>
#include "meta/node.h"
#include "meta/schema.h"
#include "table/mrow.h"
#include "table/mtable.h"

namespace surfingdb {
namespace connector {
using namespace surfingdb::meta;
using namespace surfingdb::table;

/**
 * thin kafka client wrapper
 * https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html
 */
class Connector {
public:
  std::string connector_name;
  size_t max_batch_size;
  int timeout;
  std::shared_ptr<mschema> schema_ptr;
  Connector(
    const std::shared_ptr<node> node_ptr,
    std::string connector_name,
    size_t max_batch_size,
    int timeout,
    std::shared_ptr<mschema> schema_ptr,
    std::function<std::shared_ptr<mrow>(const char* payload, const mschema& schema)> deser) {
    this->node_ptr = node_ptr;
    this->connector_name = connector_name;
    this->max_batch_size = max_batch_size;
    this->timeout = timeout;
    this->schema_ptr = schema_ptr;
    this->deser = deser;
  }
  Connector() {}

  /**
   * @brief method that poll records into a mtable
   *
   * @param max_batch_size max size of table returned
   * @param timeout max time in milliseconds for each poll
   * @param schema_ptr schema of table returned
   * @param deser function that convert payload binary to a mrow with schema_ptr
   * @return std::shared_ptr<mtable> micro batch table
   */
  virtual std::shared_ptr<mtable> consume_batch() = 0;

  /**
   * @brief
   *
   * @param timeout max time used push data out
   * @param output micro batch table
   * @param ser serialize each row in output and push out
   * @return size_t total number of rows published
   */
  size_t produce_batch() {
    return 0;
  }

protected:
  std::shared_ptr<node> node_ptr;
  std::function<std::shared_ptr<mrow>(const char* payload, const mschema& schema)> deser;
};
} // namespace connector

} // namespace surfingdb
#endif // SURFINGDB_CONNECTOR_H