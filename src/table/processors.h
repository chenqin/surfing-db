//
// Created by cq on 2/10/21.
//

#ifndef SURFINGDB_PROCESSORS_H
#define SURFINGDB_PROCESSORS_H
#include "mtable.h"
#pragma once

namespace surfingdb {
namespace table {
/**
 * transform rows in mtable in to same number pardo rows in mtable out on by one
 */
  class processors {
  public:
    static void pardo(mtable& in, mtable &out, std::function<void(const RowBuffer&, RowBuffer&)> transform) {
      out.reserveRow(in.row_size());
      for(size_t i = 0 ; i < in.row_size(); i++) {
        auto in_row = in.readRow(i);
        RowBuffer out_row(out.getSchema());
        transform(*in_row.get(), out_row);
        CHECK_EQ(out_row.schema_sig(), out.getSchema()->schema_sig());
        out.appendRow(out_row);
      }
    }
  };
}
}
#endif //SURFINGDB_PROCESSORS_H
