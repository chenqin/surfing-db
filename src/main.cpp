/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "../include/main.h"
#include "table/table.h"
#include <glog/logging.h>
#include <iostream>

class mychunk {
  public:
    int a;
    long b;

    static std::shared_ptr<arrow::Schema> schema() {
        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("a", arrow::int32()),
                arrow::field("b", arrow::int64())};

        return std::make_shared<arrow::Schema>(schema_vector);
    }

    // friends defined inside class body are inline and are hidden from non-ADL lookup
    friend mychunk operator+(mychunk lhs,        // passing lhs by value helps optimize chained a+b+c
                             const mychunk &rhs) // otherwise, both parameters may be const references
    {
        //should read from payload and decide what to do
        if (rhs.a == lhs.a) {
            lhs.b += 1;
        }
        return lhs; // return the result by value (uses move constructor)
    }
};

/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main() {
    //google::InitGoogleLogging(argv[0]);
    // create node of cluster
    const auto node = std::make_shared<surfingdb::table::Node>();
    // define a row table bind to each node
    surfingdb::table::RowTable<mychunk> t(node);
    //std::cout << "watermark is " << t.watermark() << std::endl;
    auto col = std::make_shared<surfingdb::table::ColumnarTable<mychunk>>();

    mychunk c;
    t.regType(c.schema());

    c.b = (long) node->rank;
    c.a = node->rank;

    t.send(0, 1, c);
    if(node->rank == 1) {
        std::cout << "rank 1 recv " << c.b << c.a << std::endl;
    }
    std::vector<mychunk> chunks;

    for(long i = 0 ; i < 10000000 ; i++) {
        chunks.push_back(c);
    }
    t.ingest(chunks);

    MPI_Barrier(MPI_COMM_WORLD);
    double t1, t2;
    t1 = MPI_Wtime();
    t.allreduce(chunks);

    t2 = MPI_Wtime();
    if(node->rank == 0) {
        LOG(INFO) <<  "all reduce cost " << t2 - t1;
    }
    t.flush(col);

    std::vector<mychunk> chunks2;
    int vol = 100000000 / node->world + 1;
    for(long i = 0 ; i < vol ; i++) {
        c.a = i;
        chunks2.push_back(c);
    }
    t.ingest(chunks2);
    t1 = MPI_Wtime();

    std::function<int(const int&, const int&, const mychunk&)> ope =
            [=](const int &rank, const int &world, const mychunk &s) { return (s.a + rank) % world; };
    surfingdb::table::PartitionOp<mychunk> partitionOp(node->rank, node->world, t.row_type, ope);

    t.partition(partitionOp);
    t2 = MPI_Wtime();
    if(node->rank == 0) {
        LOG(INFO) << "shuffle " << vol * node->world << " on " << node->world << " workers costs "
                  << t2 - t1;
    }

    t.flush(col);
    return 0;
}
