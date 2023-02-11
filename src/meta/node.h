/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <iostream>
#include <memory>
#include <mpi.h>
#include <vector>

#ifndef SURFINGDB_NODE_H
#define SURFINGDB_NODE_H
namespace surfingdb {
namespace meta {

class node {
private:
  bool* issubscriber;

public:
  node(int* argc, char*** argv);
  node(int, int, std::string);
  void setIsSubscriber(bool*);
  bool getIsSubscriber();
  ~node();
  long forward(); // move to next stage of compute
  int world;
  int rank;
  long stage;
  std::string processor;
};
} // namespace meta
} // namespace surfingdb
#endif // SURFINGDB_NODE_H
