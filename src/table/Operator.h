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
           FOOBAR,
           ParDo,
           GroupByKey,
           CoGroupByKey,
           Combine,
           Partition, // in place shuffle to each process
           STATISTIC  // combined sketch stream columnar value distribution
        };
        template<class RowInL, class RowInR, class RowOut>
        class Operator {
        protected:
            /**
             * OperatorType
             */
            OperatorType _type;
            /**
             * MPI_DATATYPE of left, right and output
             */
            MPI_Datatype _type_l, _type_r, _type_out;
        public:
            Operator(){
                _type = FOOBAR;
            }

            Operator(OperatorType type){
                _type = type;
            }
            ~Operator() {

            }
            OperatorType Optype() {
                return this->_type;
            }
            /**
             * apply udf to left and right vector of rows and generate result
             * @param rowInL left rows
             * @param rowInR right rows
             * @param rowOut otuput rows
             */
            virtual void process(const std::vector<RowInL>& rowInL,const std::vector<RowInR>& rowInR, std::vector<RowOut>& rowOut) = 0;
        };
    }
    }
#endif //SURFINGDB_OPERATOR_H
