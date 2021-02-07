//
// Created by cq on 2/6/21.
//

#ifndef SURFINGDB_MTABLE_H
#define SURFINGDB_MTABLE_H
#pragma once

#include "Node.h"
#include "XGBOperator.h"

namespace surfingdb {
namespace table {
using surfingdb::node::Node;

class mtable {
private:
  MPI_Win win;
  std::vector<uint8_t> payload;

  // defines the node row table bind to
  std::shared_ptr<Node> node_ptr;
  std::shared_ptr<TableSchema> schema_ptr;
public:
  mtable(const std::shared_ptr<Node>, const std::shared_ptr<TableSchema>);
  ~mtable();
};

} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_MTABLE_H
