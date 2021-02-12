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
/**
 * transform rows in mtable in to same number map rows in mtable out on by one
 */
  class processors {
  public:
    static void map(mtable& in, mtable &out, std::function<void(const RowBuffer&, RowBuffer&)> transform) {
      out.reserveRow(in.row_size());
      for(size_t i = 0 ; i < in.row_size(); i++) {
        auto in_row = in.readRow(i);
        RowBuffer out_row(out.getSchema());
        transform(*in_row.get(), out_row);
        CHECK_EQ(out_row.schema_sig(), out.getSchema()->schema_sig());
        out.appendRow(out_row);
      }
    }
    static void xgb(mtable& in, std::vector<Field> features, Field& label, const XGBParameters& parameters) {
      xgbop op(features, label, parameters, in.getNodePtr()->rank, in.getNodePtr()->world);
      std::vector<float> features_matrix;
      features_matrix.resize(op.features() * in.row_size()); // number of features
      in.readFields(op.fields, &features_matrix[0]);        // read from temp table

      std::vector<float> label_matrix; // number of labels
      label_matrix.resize(in.row_size());  // number of rows

      if (op.parameters.isTraining) {
        in.readField(op.labelField, &label_matrix[0]);
        size_t total_row_count = in.row_size();

        op.gather(&features_matrix[0], &label_matrix[0], total_row_count, op.features()); //gather training dataset to root
        op.train(&features_matrix[0], &label_matrix[0], total_row_count, op.features());
        op.syncModel(); // send model to all processes from root
      } else {
        op.predict(&features_matrix[0], &label_matrix[0], in.row_size(), op.features());
        in.writeField(op.labelField, &label_matrix[0]);
      }
    }
  };
}
}
#endif //SURFINGDB_PROCESSORS_H
