//
// Created by Chen Qin on 12/31/20.
//

#include "table.h"
#include <mpi.h>
#include <glog/logging.h>
#include <chrono>

namespace surfingdb {
    namespace table {
        long RowTable::watermark() noexcept {
            _watermark = this->ptr->rank;
            long _gLowWatermark;
            MPI_Allreduce(&_watermark, &_gLowWatermark, 1, MPI_LONG, MPI_MIN, MPI_COMM_WORLD);
            return _gLowWatermark;
        }

        RowTable::RowTable(const std::shared_ptr<Node> node) noexcept {
            this->ptr = node;
        }

        void RowTable::send(int source, int dest, const mychunk& data) {
            if (this->ptr->rank == source) {
                MPI_Send((void *) &data, 1, data.datatype, dest, 0, MPI_COMM_WORLD);
            } else if (this->ptr->rank == dest) {
                MPI_Recv((void *) &data, 1, data.datatype, source, 0, MPI_COMM_WORLD,
                         MPI_STATUS_IGNORE);
            }
        }

        void RowTable::sendAll(int source, int dest, const std::vector<mychunk> &chunks) {
            assert(!chunks.empty());

            if (this->ptr->rank == source) {
                MPI_Send((void *) &chunks[0], chunks.size(), chunks.at(0).datatype, dest, 0, MPI_COMM_WORLD);
            } else if (this->ptr->rank == dest) {
                MPI_Recv((void *) &chunks[0], chunks.size(), chunks.at(0).datatype, source, 0, MPI_COMM_WORLD,
                         MPI_STATUS_IGNORE);
            }
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