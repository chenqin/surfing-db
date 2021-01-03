//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include <flatbuffers/flatbuffers.h>
#include <mpi.h>

namespace surfingdb {
    namespace table {
        /**
         * a bit of system information, wrap around mpi
         */
        class Node {
        public:
            Node();
            ~Node();
            int world;
            int rank;
            std::string processor;
        };

        class mychunk{
        public:
            int a;
            long b;
            MPI_Datatype datatype;
            void reg() {
                int count = 3;
                int array_of_blocklengths[] = { 1, 1, 1};
                MPI_Aint array_of_displacements[] = { offsetof( mychunk, a ),
                                                      offsetof( mychunk, b ),
                                                      offsetof(mychunk, datatype)};
                MPI_Datatype array_of_types[] = { MPI_INT, MPI_LONG, MPI_INT };
                MPI_Datatype tmp_type;
                MPI_Aint lb, extent;

                MPI_Type_create_struct( count, array_of_blocklengths, array_of_displacements,
                                        array_of_types, &tmp_type );
                MPI_Type_get_extent( tmp_type, &lb, &extent );
                MPI_Type_create_resized( tmp_type, lb, extent, &datatype );
                MPI_Type_commit( &datatype );

            }
        };
        /**
         * row table is created collectively on all nodes
         * it hold vector of flatbuffer instances read from ingestion side
         * usually from partitioned kafka or s3 files
         */
        class RowTable {
        private:
            // defines the node row table bind to
            std::shared_ptr<Node> ptr;
            // low watermark of entire table
            long _watermark;
        public:
            RowTable(const std::shared_ptr<Node>) noexcept;
            /**
             * @return low watermark across all partitions to infer data completeness
             */
            long watermark() noexcept;
            /**
             * blocking send chunk of data to another node
             */
            void send(int, int, const mychunk&);
            /**
             * blocking send vector of struct mychunk
             */
            void sendAll(int, int, const std::vector<mychunk>&);
        };
        /**
         * columnarTable is maintained collectively to as one logical arrow table
         */
        class ColumnarTable {

        };
    }
}
#endif //SURFINGDB_TABLE_H
