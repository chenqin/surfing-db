//
// Created by cq on 2/10/21.
//

#ifndef SURFINGDB_PROCESSORS_H
#define SURFINGDB_PROCESSORS_H

#include "mtable.h"
#include "xgbop.h"

#pragma once

namespace surfingdb {
namespace table {
class processors {
public:
  static std::shared_ptr<mtable> map(std::shared_ptr<mtable>, std::shared_ptr<TableSchema>, std::function<void(const RowBuffer&, RowBuffer&)>);

  static void reduce(std::shared_ptr<mtable>, Field&, std::shared_ptr<std::unordered_map<Value , std::shared_ptr<RowBuffer>, ValueHasher>> result_ptr, std::shared_ptr<TableSchema> reduce_schema_ptr, std::function<void(Value&,std::vector<std::unique_ptr<RowBuffer>>&, std::shared_ptr<RowBuffer>&)>);

  static std::shared_ptr<mtable> shuffleRMA(std::shared_ptr<mtable>, Field&);

  static std::shared_ptr<mtable> shuffle(std::shared_ptr<mtable>, Field&);

  static void xgb(std::shared_ptr<mtable>, std::vector<Field>, Field&, const XGBParameters&);
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_PROCESSORS_H
