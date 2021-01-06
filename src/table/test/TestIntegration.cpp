//
// Created by Chen Qin on 12/31/20.
//
#include <gtest/gtest.h>
#include "table/table.h"
#include "table/Operator.h"
namespace surfingdb{
    namespace table {
        namespace test {
            struct myDummy {
                int a;
            };
          TEST(TableTest, TestAloha) {
              std::vector<myDummy> dataIn, dataInR, dataOut;
                std::function<int(const int&, const int&, const myDummy &)> partitioner =
                        [=](const int& rank, const int& world, const myDummy &s) { return (s.a+rank) % world; };
              PartitionOp<myDummy> op1(0,1, MPI_INT, partitioner);
          }
}
}
}