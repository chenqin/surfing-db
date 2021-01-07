//
// Created by cq on 1/6/21.
//

#ifndef SURFINGDB_PARDOOP_H
#define SURFINGDB_PARDOOP_H

#include "Operator.h"
#include <omp.h>
#include <mpi.h>

#pragma once

namespace surfingdb {
    namespace table {
        using namespace arrow;
        using namespace std;

        /**
         * "map" after reshuffle
         * transform list of row from L to Out
         * transform list of row from L & R to Out
         * @tparam RowInL
         * @tparam RowInR
         * @tparam RowOut
         */
        class ParDoOp : public Operator<arrow::Buffer, arrow::Buffer, arrow::Buffer> {
        private:
            shared_ptr<Schema> rL, rR, rOut;
            function<void(const shared_ptr<Schema>, const shared_ptr<Schema>, const shared_ptr<Schema>,
                          const std::shared_ptr<arrow::Buffer>, const std::shared_ptr<arrow::Buffer>,
                          std::shared_ptr<arrow::Buffer>)> _doFunc;
        public:
            ParDoOp(const shared_ptr<Schema> rowL, const shared_ptr<Schema> rowR, const shared_ptr<Schema> rowOut,
                    std::function<void(const shared_ptr<Schema>, const shared_ptr<Schema>, const shared_ptr<Schema>,
                                       const shared_ptr<arrow::Buffer>, const std::shared_ptr<arrow::Buffer>,
                                       shared_ptr<arrow::Buffer>)> &doFunc)
                    : Operator<arrow::Buffer, arrow::Buffer, arrow::Buffer>() {
                rL = rowL;
                rR = rowR;
                rOut = rowOut;
                this->_doFunc = doFunc;
                this->_type = ParDo;
            }

            void process(const std::vector<arrow::Buffer> &rowInL, const std::vector<arrow::Buffer> &rowInR,
                         std::vector<arrow::Buffer> &rowOut) {
                LOG(INFO) << rowInL.size() << rowOut.size() << rowInR.size();
            }

            void process(const std::vector<std::shared_ptr<arrow::Buffer>> rowInL,
                         const std::vector<std::shared_ptr<arrow::Buffer>> rowInR,
                         std::vector<std::shared_ptr<arrow::Buffer>> rowOut) {
                assert(rowInL.size() == rowInR.size());
                size_t total = rowInL.size();
                rowOut.resize(total);

#pragma omp parallel for private(rL, rR, rOut)
                for (size_t i = 0; i < total; i++) {
                    _doFunc(rL, rR, rOut, rowInL.at(i), rowInR.at(i), rowOut.at(i));
                }
            }
        };
    }
}
#endif //SURFINGDB_PARDOOP_H
