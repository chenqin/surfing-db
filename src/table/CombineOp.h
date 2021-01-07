//
// Created by cq on 1/5/21.
//

#ifndef SURFINGDB_COMBINEOP_H
#define SURFINGDB_COMBINEOP_H

#include "Operator.h"
#include <mpi.h>

#pragma once

namespace surfingdb {
    namespace table {
        using namespace arrow;
        using namespace std;
        /**
         * combine two same typed rows into one
         * @tparam Row
         */
        class CombineOp : public Operator<arrow::Buffer, arrow::Buffer, arrow::Buffer> {
        public:
            CombineOp() : Operator<arrow::Buffer, arrow::Buffer, arrow::Buffer>() {
                this->_type = OperatorType::Combine;
            }

            void process(const std::vector<std::shared_ptr<arrow::Buffer>> rowInL,
                         const std::vector<std::shared_ptr<arrow::Buffer>> rowInR,
                         std::vector<std::shared_ptr<arrow::Buffer>> rowOut) {
                rowOut.resize(rowInR.size() + rowInL.size());
                // check memory
                if (rowOut == rowInL) {
                    rowOut.insert(rowOut.end(), rowInR.begin(), rowInR.end());
                } else if (rowOut == rowInR) {
                    rowOut.insert(rowOut.end(), rowInL.begin(), rowInL.end());
                } else {
                    rowOut.clear();
                    rowOut.insert(rowOut.end(), rowInL.begin(), rowInL.end());
                    rowOut.insert(rowOut.end(), rowInR.begin(), rowInR.end());
                }
            }

            void process(const std::vector<arrow::Buffer> &rowInL, const std::vector<arrow::Buffer> &rowInR,
                         std::vector<arrow::Buffer> &rowOut) {
                LOG(INFO) << rowInL.size() << rowOut.size() << rowInR.size();
            }
        };
    }
}
#endif //SURFINGDB_COMBINEOP_H
