//
// Created by cq on 1/5/21.
//

#ifndef SURFINGDB_OPERATOR_H
#define SURFINGDB_OPERATOR_H
#include <glog/logging.h>

namespace surfingdb {
    namespace table {
        /**
         * define list of operator surfingdb supports
         * https://beam.apache.org/documentation/programming-guide/#pardo
         */
        enum OperatorType {
           ParDo,
           GroupByKey,
           CoGroupByKey,
           Combine,
           Flattern,
           Partition
        };
        template<class RowInL, class RowInR, class RowOut>
        class Operator {
        private:
            OperatorType _type;
        public:
            Operator(OperatorType type){
                this->type = type;
            }
            ~Operator() {

            }
            OperatorType type() {
                return this->_type;
            }
            /**
             * apply udf to left and right vector of rows and generate result
             * @param rowInL left rows
             * @param rowInR right rows
             * @param rowOut otuput rows
             */
            void process(const std::shared_ptr<std::vector<RowInL>> rowInL, const std::shared_ptr<std::vector<RowInR>> rowInR, std::vector<RowOut> rowOut) {
                LOG(INFO) << "left row " << rowInL.size();
                LOG(INFO) << "right row" << rowInR.size();
                LOG(INFO) << "output row" << rowOut.size();
            }
        };
    }
    }
#endif //SURFINGDB_OPERATOR_H
