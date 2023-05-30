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

#ifndef MATCHA_DATAGEN_H
#define MATCHA_DATAGEN_H

#include <rdkafka.h>

#include <string>
#include <vector>

#include "connector/connector.h"
#include "meta/node.h"
#include "meta/schema.h"
#include "table/mrow.h"
#include "table/mtable.h"

namespace matcha {
namespace connector {
using namespace std;
using namespace matcha::meta;
using namespace matcha::table;

/**
 * @brief data generator
 *
 */
class DataGenConnector : public Connector {
 public:
  DataGenConnector(const std::shared_ptr<node> node_ptr,
                   std::string connector_name, size_t max_batch_size,
                   int timeout, std::shared_ptr<mschema> schema_ptr);

  shared_ptr<mtable> consume_batch(
      std::function<std::shared_ptr<mrow>(const char* payload,
                                          const mschema& schema)>
          deser);
  std::shared_ptr<arrow::RecordBatch> consume_batch(
      std::function<
          size_t(const void* payload, size_t len,
               std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)>
          deser);
};
}  // namespace connector

}  // namespace matcha
#endif  //  MATCHA_DATAGEN_H
