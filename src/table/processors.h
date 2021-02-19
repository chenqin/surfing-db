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
  static void map(std::shared_ptr<mtable>, std::shared_ptr<mtable>, std::function<void(const RowBuffer&, RowBuffer&)>);

  static std::shared_ptr<mtable> partition(std::shared_ptr<mtable>, Field&);

  static void xgb(std::shared_ptr<mtable>, std::vector<Field>, Field&, const XGBParameters&);
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_PROCESSORS_H
