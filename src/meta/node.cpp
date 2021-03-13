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

node::node(int* argc, char ***argv) {
  // Initialize the MPI environment
  int supported;
  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &supported);
  CHECK_EQ(supported, MPI_THREAD_MULTIPLE);
  MPI_Comm_size(MPI_COMM_WORLD, &this->world);
  MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);
  MPI_Get_processor_name(processor_name, &name_len);

  processor = std::string(processor_name);
  stage = 0;
  outstanding_requests.clear();
	duckdb::DBConfig config;
	config.force_checkpoint = true;
	config.maximum_memory = 2*1024*1024*1024;
	db_ptr = std::make_shared<duckdb::DuckDB>(nullptr, &config);
	this->db_con = std::make_shared<duckdb::Connection>(*db_ptr.get());
  // LOG(INFO) << "cluster size " << world << " node rank " << rank << " alias " << processor;
}

node::~node() {
  // Finalize the MPI environment.
  MPI_Finalize();
  //LOG(INFO) << "cluster finalized";
}

/**
 * all processes move to next stage
 * @return
 */
long node::forward() {
  for(auto& r : outstanding_requests) {
    MPI_Status status;
    MPI_Wait(r.get(), &status);
  }
  outstanding_requests.clear();

  long max_stage = 0;
  MPI_Allreduce(&stage, &max_stage, 1, MPI_LONG, MPI_MAX, MPI_COMM_WORLD);
  CHECK_EQ(max_stage, stage);
  return ++stage;
}
void node::keep(std::unique_ptr<MPI_Request> req) {
  outstanding_requests.push_back(std::move(req));
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
  outstanding_requests.clear();
}
} // namespace node
} // namespace surfingdb
