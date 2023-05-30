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

namespace matcha {
namespace meta {

node::node(int* argc, char*** argv, std::string jar) {
  // Initialize the MPI environment
  int supported;
  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  /**
   * @brief check if pytorch already run initialize
   *
   */
  MPI_Initialized(&mpi_inited);
  if (!mpi_inited) {
    //TODO: tcp doesn't support RDMA with MPI_THREAD_SINGLE, only EFA works
    MPI_Init_thread(argc, argv, MPI_THREAD_SINGLE, &supported);
    CHECK_EQ(supported, MPI_THREAD_SINGLE);
  }
  MPI_Comm_size(MPI_COMM_WORLD, &this->world);
  MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);
  MPI_Get_processor_name(processor_name, &name_len);
  
  omp_set_num_threads(4);

  processor = std::string(processor_name);
  MPI_Info_create(&info);
  CHECK(MPI_Info_set(info, "no_locks", "true") == 0);

  std::cout << processor << " on " << rank << std::endl;
  stage = 0;
  LOG(INFO) << "cluster size " << world << " node rank " << rank << " alias " << processor << " total threads " << omp_get_num_threads();
  trainer = 0;

  MPI_Comm_split(MPI_COMM_WORLD, trainer, rank, &role_comm);
  MPI_Comm_rank(role_comm, &role_rank);
  MPI_Comm_size(role_comm, &role_world);
  /*
  JavaVMInitArgs vm_args;
  JavaVMOption options[3];
  std::string s = "-Djava.class.path=" + jar;
  options[0].optionString = (char*) s.c_str();
  options[1].optionString = "-XX:+UseG1GC"; //user full gc to avoid memory leak
  //options[1].optionString = "-verbose:jni";
  vm_args.version = JNI_VERSION_1_8;
  vm_args.nOptions = 2;
  vm_args.options = options;
  vm_args.ignoreUnrecognized = JNI_TRUE;
  int status = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
  if (status < 0) {
    std::cerr << "\n<<<<< Unable to Launch JVM >>>>>\n"
              << std::endl;
    env = nullptr;
  }
  CHECK_NOTNULL(env);
  */
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
} // namespace matcha
