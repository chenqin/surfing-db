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
  static void map(mtable& in, mtable& out, std::function<void(const RowBuffer&, RowBuffer&)> transform);

  static void partition(mtable& in, Field& f);

  static void xgb(mtable& in, std::vector<Field> features, Field& label, const XGBParameters& parameters);
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_PROCESSORS_H
