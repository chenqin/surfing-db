//
// Created by cq on 1/4/21.
//
#include <iostream>
#include <vector>
#include <memory>
#include <mpi.h>

#ifndef SURFINGDB_NODE_H
#define SURFINGDB_NODE_H
namespace surfingdb {
namespace meta {

class node {
public:
  node(int* argc, char ***argv);
  node(int, int, std::string);
  ~node();

  long forward(); // move to next stage of compute
  void keep(std::unique_ptr<MPI_Request>);
  int world;
  int rank;
  long stage;
  std::vector<std::unique_ptr<MPI_Request>> outstanding_requests;
  std::string processor;
};
} // namespace meta
} // namespace surfingdb
#endif //SURFINGDB_NODE_H
