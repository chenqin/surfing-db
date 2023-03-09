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

#include "node.h"
#include <glog/logging.h>

namespace surfingdb {
namespace meta {

void node::setissubscriber(bool* is) {
  CHECK_NOTNULL(is);
  this->issubscriber = is;
}

int node::getissubscriber() {
  if (issubscriber == nullptr) return 0;
  return *issubscriber ? 1 : -1;
}

node::node(int* argc, char*** argv) {
  // Initialize the MPI environment
  int supported;
  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  /**
   * @brief check if pytorch already run initialize
   * 
   */
  int flag = 0;
  MPI_Initialized(&flag);
  if (!flag) {
    MPI_Init_thread(argc, argv, MPI_THREAD_FUNNELED, &supported);
    CHECK_EQ(supported, MPI_THREAD_FUNNELED);
  }
  MPI_Comm_size(MPI_COMM_WORLD, &this->world);
  MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);
  MPI_Get_processor_name(processor_name, &name_len);

  processor = std::string(processor_name);
  std::cout << processor << " on " << rank << std::endl;
  stage = 0;
  LOG(INFO) << "cluster size " << world << " node rank " << rank << " alias " << processor;

  // TODO: FIXME determine role of each node
  trainer = rank % 4 == 0 ? 1 : 0; // 

  MPI_Comm_split(MPI_COMM_WORLD, trainer, rank, &role_comm);
  MPI_Comm_rank(role_comm, &role_rank);
  MPI_Comm_size(role_comm, &role_world);

  JavaVMInitArgs vm_args;
  JavaVMOption options[2];
  options[0].optionString = "-Djava.class.path=../surfing-db-java.jar";
  options[1].optionString = "-DXcheck:jni:pedantic";
  vm_args.version = JNI_VERSION_1_8;
  vm_args.nOptions = 2;
  vm_args.options = options;
  int status = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
  if (status < 0) {
    std::cerr << "\n<<<<< Unable to Launch JVM >>>>>\n"
              << std::endl;
    env = nullptr;
  }
  CHECK_NOTNULL(env);
}

/**
 * all processes move to next synchorinization stage
 * @return
 */
long node::forward() {
  long max_stage = 0;
  MPI_Allreduce(&stage, &max_stage, 1, MPI_LONG, MPI_MAX, MPI_COMM_WORLD);
  CHECK_EQ(max_stage, stage);
  return ++stage;
}
/**
 * for testing use
 * @param rank
 * @param world
 * @param processor
 */
node::node(int rank, int world, std::string processor) {
  this->rank = rank;
  this->world = world;
  this->processor = processor;
  stage = 0;
}
} // namespace meta
} // namespace surfingdb
