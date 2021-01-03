//
// Created by Chen Qin on 12/31/20.
//

#include "table.h"
#include <mpi.h>
#include <glog/logging.h>
#include <omp.h>

namespace surfingdb {
    namespace table {
        /**
         * use class operator+ to get row count of a key
         * @tparam T
         * @param a
         * @param b
         * @param len
         */
        template<class T>
        void reducer(void *a, void *b, int *len, MPI_Datatype *)
        {
            T *aa=static_cast<T *>(a);
            T *bb=static_cast<T *>(b);
#pragma omp simd
            for (int i=0; i<*len; ++i) {
                bb[i] = aa[i] + bb[i];
            }
#pragma omp barrier
        }



        long RowTable::watermark() noexcept {
            _watermark = this->ptr->rank;
            long _gLowWatermark;
            MPI_Allreduce(&_watermark, &_gLowWatermark, 1, MPI_LONG, MPI_MIN, MPI_COMM_WORLD);
            return _gLowWatermark;
        }

        RowTable::RowTable(const std::shared_ptr<Node> node) noexcept {
            this->ptr = node;
            this->_chunks = std::make_shared<std::vector<mychunk>>();
            // create list of user defined MPI_OPs
            MPI_Op_create(&reducer<mychunk>, true, &this->sum);
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

        void RowTable::allreduce(const std::vector<mychunk> &chunks, const MPI_Op& op) {
            std::vector<mychunk> result(chunks.size());
            // used to collective get quantile sketch of each columns https://datasketches.apache.org/docs/Quantiles/QuantilesCppExample.html
            MPI_Allreduce((void *) &chunks[0], (void *) &result[0], chunks.size(), chunks.at(0).datatype, op, MPI_COMM_WORLD);
        }

        RowTable::~RowTable() {
            MPI_Op_free(&this->sum);
        }

        void RowTable::shuffle(std::vector<mychunk> &chunks) {
            std::vector<mychunk> lists[ptr->world];
            for (size_t i = 0 ; i < chunks.size(); i++) {
                auto shard = chunks[i].a % ptr->world;
                lists[shard].push_back(chunks[i]);
            }
            chunks.clear();
            MPI_Barrier(MPI_COMM_WORLD);

            for(int j = 0 ; j < ptr->world; j++) {
                if(j != ptr->rank) {
                    // send to corrsponding node
                    long size = lists[j].size();
                    MPI_Send(&size, 1, MPI_LONG, j, 0, MPI_COMM_WORLD);
                    if(size != 0) {
                        MPI_Send((void *) &lists[j][0], lists[j].size(), lists[j].at(0).datatype, j, 0, MPI_COMM_WORLD);
                    }
                } else {
                   for(size_t num = 0 ; num < lists[j].size(); num++) {
                        chunks.push_back(lists[j].at(num));
                   }
                }
            }

            for(int j = 0 ; j < ptr->world; j++) {
                if(j != ptr->rank) {
                    long recv = 0;
                    MPI_Recv(&recv, 1, MPI_LONG, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    if(recv > 0) {
                        std::vector<mychunk> temp(recv);
                        MPI_Recv((void *) &temp[0], temp.size(), temp.at(0).datatype, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        for (long num = 0; num < recv; num++) {
                            chunks.push_back(temp.at(num));
                        }
                    }
                }
            }
            // shuffle complete in all nodes
            MPI_Barrier(MPI_COMM_WORLD);
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