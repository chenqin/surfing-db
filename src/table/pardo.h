//
// Created by cq on 2/10/21.
//

#ifndef SURFINGDB_PARDO_H
#define SURFINGDB_PARDO_H
#include "mtable.h"
#pragma once

namespace surfingdb {
namespace table {
/**
 * transform rows in mtable in to same number of rows in mtable out on by one
 */
  class pardo {
  public:
    static void of(mtable& in, mtable &out, std::function<void(const RowBuffer&, RowBuffer&)> tranform) {
      out.readRow(in.row_size());
      for(int i = 0 ; i < in.row_size(); i++) {
        auto in_row = in.readRow(i);
        RowBuffer out_row(out.getSchema());
        tranform(*in_row.get(), out_row);
        out.appendRow(out_row);
      }
    }
  };
}
}
#endif //SURFINGDB_PARDO_H
