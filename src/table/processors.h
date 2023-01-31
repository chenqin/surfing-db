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

#ifndef SURFINGDB_PROCESSORS_H
#define SURFINGDB_PROCESSORS_H

#include <arrow/api.h>
#include "mtable.h"
#include "xgbop.h"

#pragma once

namespace surfingdb {
namespace table {
class processors {
public:
  /**
   * map convert mtable to new table with schema ptr, if map function return true, otherwise current row
   */
  static std::shared_ptr<mtable> map(std::shared_ptr<mtable>, std::shared_ptr<TableSchema>, std::function<bool(const RowBuffer&, RowBuffer&)>);

  static void reduce(std::shared_ptr<mtable>, Field&, std::shared_ptr<std::unordered_map<Value, std::shared_ptr<RowBuffer>, ValueHasher>> result_ptr, std::shared_ptr<TableSchema> result_schema_ptr, std::function<void(Value&, std::vector<std::unique_ptr<RowBuffer>>&, std::shared_ptr<RowBuffer>&)>);

  static std::shared_ptr<mtable> shuffleRMA(std::shared_ptr<mtable>, Field&);

  static std::shared_ptr<mtable> shuffle(std::shared_ptr<mtable>, Field&, bool);

  static void xgb(std::shared_ptr<mtable>, std::vector<Field>, Field&, const XGBParameters&);
};
} // namespace table
} // namespace surfingdb
#endif // SURFINGDB_PROCESSORS_H
