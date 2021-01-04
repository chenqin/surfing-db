//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include <arrow/api.h>
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
            static std::shared_ptr<arrow::Schema> getArrowSchema() {
                std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                        arrow::field("a", arrow::int32()), arrow::field("b", arrow::int64())};

                return std::make_shared<arrow::Schema>(schema_vector);
            }
            int rank(int world) {
                return a%world;
            }
            // friends defined inside class body are inline and are hidden from non-ADL lookup
            friend mychunk operator+(mychunk lhs,        // passing lhs by value helps optimize chained a+b+c
                               const mychunk& rhs) // otherwise, both parameters may be const references
            {
                //should read from payload and decide what to do
                if(rhs.a == lhs.a) {
                    lhs.b += 1;
                }
                return lhs; // return the result by value (uses move constructor)
            }
        };
        /**
         * row table is created collectively on all nodes
         * it hold vector of flatbuffer instances read from ingestion side
         * usually from partitioned kafka or s3 files
         */

        class RowTable{
        private:
            // defines the node row table bind to
            std::shared_ptr<Node> ptr;
            //TODO(chenqin): use RMA , list of chunks stored,
            std::shared_ptr<std::vector<mychunk>> _chunks;
            // low watermark of entire table
            long _watermark;
        public:
            MPI_Op op;
            MPI_Datatype type = 0;
            void regType(const mychunk&) {
                if(type == 0) {
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
                    MPI_Type_create_resized( tmp_type, lb, extent, &type );
                    MPI_Type_commit( &type );
                }
            }

            ~RowTable();
            explicit RowTable(const std::shared_ptr<Node>) noexcept;
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
            void sendAll(int, int, int, const std::vector<mychunk>&);

            void allreduce(const std::vector<mychunk>&, const MPI_Op&);
            /**
             * shuffle and place record to rank() node
             */
            void shuffle(std::vector<mychunk>&, std::function<int(mychunk)>);
        };
        /**
         * columnarTable is maintained collectively to as one logical arrow table
         */
        class ColumnarTable {
        private:
            std::shared_ptr<arrow::Table> tableptr;
        public:
            ColumnarTable();
            void toTable(const std::vector<mychunk>&);
        };
    }
}
#endif //SURFINGDB_TABLE_H
