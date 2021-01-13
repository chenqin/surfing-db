//
// Created by cq on 1/12/21.
//

#ifndef SURFINGDB_KMEANOPERATOR_H
#define SURFINGDB_KMEANOPERATOR_H

#include "row.h"
#include "Node.h"

namespace surfingdb {
namespace table {

template<class BidiIter >
BidiIter random_unique(BidiIter begin, BidiIter end, size_t num_random) {
  size_t left = std::distance(begin, end);
  while (num_random--) {
    BidiIter r = begin;
    std::advance(r, rand()%left);
    std::swap(*begin, *r);
    ++begin;
    --left;
  }
  return begin;
}


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
      //CHECK(schema_ptr->exist(f));
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
