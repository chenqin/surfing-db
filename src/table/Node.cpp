//
// Created by cq on 1/4/21.
//

#include "Node.h"
#include <glog/logging.h>
#include <mpi.h>

namespace surfingdb {
namespace node {

Node::Node(int* argc, char ***argv) {
  // Initialize the MPI environment
  // MPI_Init(argc, argv);
  int supported;
  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &supported);
  CHECK_EQ(supported, MPI_THREAD_MULTIPLE);
  MPI_Comm_size(MPI_COMM_WORLD, &this->world);
  MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);
  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);
  processor = std::string(processor_name);
  stage = 0;
  istesting = false;
  outstanding_requests.clear();
  // LOG(INFO) << "cluster size " << world << " node rank " << rank << " alias " << processor;
}

Node::~Node() {
  // Finalize the MPI environment.
  MPI_Finalize();
  //LOG(INFO) << "cluster finalized";
}

/**
 * all processes move to next stage
 * @return
 */
long Node::forward() {
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
void Node::keep(std::unique_ptr<MPI_Request> req) {
  outstanding_requests.push_back(std::move(req));
}
/**
 * for testing use
 * @param rank
 * @param world
 * @param processor
 */
Node::Node(int rank, int world, std::string processor) {
  this->rank = rank;
  this->world = world;
  this->processor = processor;
  stage = 0;
  istesting = true;
  outstanding_requests.clear();
}
} // namespace node
} // namespace surfingdb
