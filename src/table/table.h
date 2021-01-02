//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include <flatbuffers/flatbuffers.h>

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
        };
        /**
         * columnarTable is maintained collectively to as one logical arrow table
         */
        class ColumnarTable {

        };
    }
}
#endif //SURFINGDB_TABLE_H
