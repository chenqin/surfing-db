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

class Group{
public:
  int _index;
  std::vector<size_t> indexes;
};

class KMeanOperator{
public:
  std::shared_ptr<TableSchema> schema_ptr;
  int k;
  std::vector<Field> fields;
  std::unordered_map<int, std::set<size_t>> groups;
  KMeanOperator(std::shared_ptr<TableSchema> schema_ptr, int k, const std::vector<Field> fields){
    CHECK_NOTNULL(schema_ptr);
    this->schema_ptr = schema_ptr;
    CHECK_GE(k, 1);
    this->k = k;
    CHECK_GT(fields.size(), 0);
    for(auto f : fields) {
      //CHECK(schema_ptr->exist(f));
      CHECK(f.type == RowType::DOUBLE); //force normalization before using
    }
    this->fields = fields;
    for(int i = 0 ; i < this->k ; i++) {
      groups.insert({i, std::set<size_t>()});
    }
  }

  void addGroup(int i, size_t index) {
    groups.at(i).insert(index);
  }

  bool shouldStop() {
    return true;
  }

  void process(RowBuffer, RowBuffer, RowBuffer) {

  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_KMEANOPERATOR_H
