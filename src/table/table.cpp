//
// Created by Chen Qin on 12/31/20.
//

#include "table.h"
#include <mpi.h>
#include <glog/logging.h>

namespace surfingdb {
    namespace table {
        long RowTable::watermark() noexcept {
            return ptr.get()->rank;
        }

        RowTable::RowTable(const std::shared_ptr<Node> node) noexcept {
            this->ptr = node;
        }

        Node::Node() {
            // Initialize the MPI environment
            MPI_Init(NULL, NULL);
            MPI_Comm_size(MPI_COMM_WORLD, &this->world);
            MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);
            // Get the name of the processor
            char processor_name[MPI_MAX_PROCESSOR_NAME];
            int name_len;
            MPI_Get_processor_name(processor_name, &name_len);
            processor = std::string(processor_name);
            LOG(INFO) << "cluster size " << world << " node rank " << rank << " alias " << processor;
        }

        Node::~Node() {
            // Finalize the MPI environment.
            MPI_Finalize();
            LOG(INFO) << "cluster finalized";
        }
    }
}