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
#include "connector/connector.h"
#include "meta/node.h"
#include "meta/schema.h"
#include "table/mtable.h"
#include "table/row.h"

namespace surfingdb {
namespace connector {
using namespace std;
using namespace surfingdb::meta;
using namespace surfingdb::table;

/**
 * @brief data generator
 *
 */
class DataGenConnector : public Connector {
public:
  DataGenConnector(const shared_ptr<node>);

  shared_ptr<mtable>
    consume_batch(size_t, int, shared_ptr<TableSchema>, function<shared_ptr<RowBuffer>(const char* payload, const TableSchema&)>);
};
} // namespace connector

} // namespace surfingdb
#endif // SURFINGDB_DATAGEN_H
