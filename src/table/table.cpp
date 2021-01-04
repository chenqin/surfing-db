//
// Created by Chen Qin on 12/31/20.
//

#include "table.h"
#include <mpi.h>
#include <glog/logging.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/file.h>
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

        void RowTable::sendAll(int source, int dest, int size, const std::vector<mychunk> &chunks) {
            assert(!chunks.empty());
            mychunk placeholder;
            placeholder.reg();
            if (this->ptr->rank == source) {
                MPI_Send((void *) &chunks[0], size, placeholder.datatype, dest, 0, MPI_COMM_WORLD);
            } else if (this->ptr->rank == dest) {
                std::vector<mychunk> results(size);
                MPI_Recv((void *) &results[0], size, placeholder.datatype, source, 0, MPI_COMM_WORLD,
                         MPI_STATUS_IGNORE);
            }
            placeholder.unreg();
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
            // organize record per rank
            std::vector<mychunk> send_buffer[ptr->world];
            int j;

            //padding
            //for(j = 0 ; j < ptr->world ; j++) {
            //    mychunk k;
            //    send_buffer[j].push_back(k);
            //}
            
            for (size_t ii = 0 ; ii < chunks.size(); ii++) {
                auto shard = chunks[ii].rank(ptr->world);
                send_buffer[shard].push_back(chunks[ii]);
            }

            chunks.clear();
            MPI_Barrier(MPI_COMM_WORLD);

            //number of records send to each rank
            int send[ptr->world];
            //number of records recv from each rank
            int recv[ptr->world];
            int total_recv;

            for(j = 0 ; j < ptr->world; j++) {
                send[j] = send_buffer[j].size();
                if(j == ptr->rank) {
                    recv[j] = send[j];
                    total_recv += recv[j];
                    continue;
                }

                MPI_Send(&send[j], 1, MPI_INT, j, 0, MPI_COMM_WORLD);
                MPI_Recv(&recv[j], 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                total_recv += recv[j];
            }
            //LOG(INFO) << "end of size exchange";
            //MPI_Barrier(MPI_COMM_WORLD);

            // place received record in array index start with 0
            int displ[ptr->world];
            displ[0] = 0;
            for(j = 1 ; j < ptr->world ; j++) {
                displ[j] = displ[j-1] + recv[j-1];
            }
            mychunk placeholder;
            placeholder.reg();
            chunks.resize(total_recv);
            // for each rank, gather rows shard to that rank
            for(j = 0 ; j < ptr->world; j++) {
                MPI_Gatherv(&send_buffer[j][0], send[j],placeholder.datatype,
                            &chunks[0], &recv[0], &displ[0], placeholder.datatype, j,
                            MPI_COMM_WORLD);
            }
            placeholder.unreg();
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

        void ColumnarTable::toTable(const std::vector<mychunk>& chunks) {
            arrow::MemoryPool* pool = arrow::default_memory_pool();
            arrow::Int32Builder a_builder(pool);
            arrow::Int64Builder b_builder(pool);
            for(auto chunk : chunks) {
                a_builder.Append(chunk.a);
                b_builder.Append(chunk.b);
            }
            std::shared_ptr<arrow::Array> a_array;
            a_builder.Finish(&a_array);
            std::shared_ptr<arrow::Array> b_array;
            b_builder.Finish(&b_array);
            tableptr = arrow::Table::Make(mychunk::getArrowSchema(), {a_array, b_array});
        }

        ColumnarTable::ColumnarTable() {

        }

        void ColumnarTable::toParquet(const std::string &path) {
            LOG(INFO) << path;
            /*
            std::shared_ptr<arrow::io::FileOutputStream> outfile;
            PARQUET_ASSIGN_OR_THROW(
                    outfile,
                    arrow::io::FileOutputStream::Open(path));
            PARQUET_THROW_NOT_OK(
                    parquet::arrow::WriteTable(*this->tableptr.get(), arrow::default_memory_pool(), outfile, 3));
                    */
        }
    }
}