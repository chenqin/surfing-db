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
        /**
         * combine two same typed rows into one
         * @tparam Row
         */
        template<typename Row>
        class CombineOp : public Operator<Row, Row, Row> {
        public:
            CombineOp() : Operator<Row, Row, Row>() {
                this->_type = OperatorType::Combine;
            }

            void process(const std::vector<Row> &rowL, const std::vector<Row> &rowR, std::vector<Row> &rowOut) {
                rowOut.resize(rowL.size() + rowOut.size());
                // check memory
                if (&rowOut == &rowL) {
                    rowOut.insert(rowOut.end(), rowR.begin(), rowR.end());
                } else if (&rowOut == &rowR) {
                    rowOut.insert(rowOut.end(), rowL.begin(), rowL.end());
                } else {
                    rowOut.clear();
                    rowOut.insert(rowOut.end(), rowL.begin(), rowL.end());
                    rowOut.insert(rowOut.end(), rowR.begin(), rowR.end());
                }
            }
        };
    }
}
#endif //SURFINGDB_COMBINEOP_H
