//
// Created by cq on 1/6/21.
//

#ifndef SURFINGDB_PARDOOP_H
#define SURFINGDB_PARDOOP_H

#include "Operator.h"
#include <omp.h>

#pragma once

namespace surfingdb {
    namespace table {
        /**
         * "map"
         * transform list of row from L to Out
         * transform list of row from L & R to Out
         * @tparam RowInL
         * @tparam RowInR
         * @tparam RowOut
         */
        template<class RowInL, class RowInR, class RowOut>
        class ParDoOp : public Operator<RowInL, RowInR, RowOut> {
        private:
            std::function<void(const RowInL &, const RowInR &, RowOut &)> _doFunc;
        public:
            ParDoOp(std::function<void(const RowInL &, const RowInR &, RowOut &)> &doFunc)
                    : Operator<RowInL, RowInR, RowOut>() {
                this->_doFunc = doFunc;
                this->_type = ParDo;
            }

            void
            process(const std::vector<RowInL> &rowInL, const std::vector<RowInR> &rowInR, std::vector<RowOut> &rowOut) {
                assert(rowInL.size() == rowInR.size());
                rowOut.resize(rowInL.size());
                size_t total = rowInL.size();
# ifdef _OPENMP
#pragma omp parallel for
# endif
                for (size_t i = 0; i < total; i++) {
                    _doFunc(rowInL.at(i), rowInR.at(i), rowOut.at(i));
                }
            }
        };
    }
}
#endif //SURFINGDB_PARDOOP_H
