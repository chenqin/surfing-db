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
namespace node {

class Node {
public:
  Node(int* argc, char ***argv);
  Node(int, int, std::string);
  ~Node();

  long forward(); // move to next stage of compute
  void keep(std::unique_ptr<MPI_Request>);
  int world;
  int rank;
  long stage;
  bool istesting = false;
  std::vector<std::unique_ptr<MPI_Request>> outstanding_requests;
  std::string processor;
};
} // namespace node
} // namespace surfingdb
#endif //SURFINGDB_NODE_H
