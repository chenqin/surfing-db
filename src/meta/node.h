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
#include <csignal>
#include <iostream>
#include <functional>
#include <memory>
#include <mpi.h>
#include <vector>
#include <jni.h>

#ifndef SURFINGDB_NODE_H
#define SURFINGDB_NODE_H
namespace surfingdb {
namespace meta {

class node {
public:
  node(int* argc, char*** argv);
  node(int, int, std::string);
  ~node(){
    MPI_Comm_free(&role_comm);
    MPI_Info_free(&info);
    MPI_Finalize();
    jvm->DestroyJavaVM();
  }
  void setissubscriber(bool*);
  /**
   * @brief if node running data input
   *
   * @return int 1 polling data in, -1 not polling data, 0 default
   */
  int getissubscriber();
  /**
   * @brief check if all nodes running at same synchoronization stage
   * 
   * @return long 
   */
  long forward(); // move to next stage of compute
  int world;
  int rank;

  /**
   * @brief each node can have one role either gpu trainer or CPU processor
   *        use role_comm to reach other nodes with same role e.g 
   *        processor shuffle or trainer allreduce gridiant avg
   *
   */
  int trainer = 0;
  MPI_Comm role_comm = MPI_COMM_NULL;
  MPI_Info info = MPI_INFO_NULL;
  int role_rank;
  int role_world;
  /**
   * @brief stage that needs golbal synchorization (e.g shuffle)
   * 
   */
  long stage;
  std::string processor;
  JavaVM* jvm;
  JNIEnv* env;
};
} // namespace meta
} // namespace surfingdb
#endif // SURFINGDB_NODE_H
