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

        void ColumnarTable::toTable(const std::vector<mychunk> &chunks) {
            arrow::MemoryPool *pool = arrow::default_memory_pool();
            arrow::Int32Builder a_builder(pool);
            arrow::Int64Builder b_builder(pool);
            for (auto chunk : chunks) {
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
    }
}