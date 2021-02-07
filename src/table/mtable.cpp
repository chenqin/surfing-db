//
// Created by cq on 2/6/21.
//
#include "mtable.h"

namespace surfingdb {
namespace table {

mtable::~mtable() {
  MPI_Win_detach(win, &payload[0]);
  payload.clear();
  payload.shrink_to_fit();
  MPI_Win_free(&win);
}

mtable::mtable(const std::shared_ptr<Node> node_ptr, const std::shared_ptr<TableSchema> schema_ptr) {
  this->schema_ptr = schema_ptr;
  this->node_ptr = node_ptr;
  payload.resize(MEM_PAGE_SIZE);
  MPI_Win_create_dynamic(MPI_INFO_NULL, MPI_COMM_WORLD, &win);
  MPI_Win_attach(win, &payload[0], MEM_PAGE_SIZE);
}
} // namespace table
} // namespace surfingdb