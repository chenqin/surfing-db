//
// Created by cq on 1/12/21.
//

#ifndef SURFINGDB_KMEANOPERATOR_H
#define SURFINGDB_KMEANOPERATOR_H
#include "row.h"

namespace surfingdb {
namespace table {

class KMeanOperator{
public:
  std::shared_ptr<TableSchema> schema_ptr;
  int k;
  std::vector<std::shared_ptr<RowBuffer>> centroids;
  std::vector<Field> fields;
  KMeanOperator(std::shared_ptr<TableSchema> schema_ptr, int k, const std::vector<Field> fields){
    CHECK_NOTNULL(schema_ptr);
    this->schema_ptr = schema_ptr;
    CHECK_GE(k, 1);
    this->k = k;
    centroids.resize(k);
    CHECK_GT(fields.size(), 0);
    for(auto f : fields) {
      CHECK(schema_ptr->exist(f));
      CHECK(f.type == RowType::DOUBLE); //force normalization before using
    }
    this->fields = fields;
  }
  void process(RowBuffer, RowBuffer, RowBuffer) {

  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_KMEANOPERATOR_H
