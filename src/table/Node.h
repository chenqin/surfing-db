//
// Created by cq on 1/4/21.
//
#include <iostream>

#ifndef SURFINGDB_NODE_H
#define SURFINGDB_NODE_H
namespace surfingdb {
namespace node {

class Node {
public:
  Node();

  ~Node();

  int world;
  int rank;
  std::string processor;
};
} // namespace node
} // namespace surfingdb
#endif //SURFINGDB_NODE_H
