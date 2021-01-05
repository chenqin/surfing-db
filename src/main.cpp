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

/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main() {
    //google::InitGoogleLogging(argv[0]);
    // create node of cluster
    const auto node = std::make_shared<surfingdb::table::Node>();
    // define a row table bind to each node
    surfingdb::table::RowTable t(node);
    //std::cout << "watermark is " << t.watermark() << std::endl;

    surfingdb::table::mychunk c;
    t.registerSchema(c.getArrowSchema());

    c.b = (long) node->rank;
    c.a = node->rank;

    t.send(0, 1, c);
    if(node->rank == 1) {
        std::cout << "rank 1 recv " << c.b << c.a << std::endl;
    }
    std::vector<surfingdb::table::mychunk> chunks;

    for(long i = 0 ; i < 10000000 ; i++) {
        chunks.push_back(c);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t1, t2;
    t1 = MPI_Wtime();
    t.allreduce(chunks, t.op);
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();
    if(node->rank == 0) {
        LOG(INFO) <<  "all reduce cost " << t2 - t1;
    }
    std::vector<surfingdb::table::mychunk> chunks2;
    int vol = 100000000 / node->world + 1;
    for(long i = 0 ; i < vol ; i++) {
        c.a = i;
        chunks2.push_back(c);
    }
    for(int j = 0 ; j < 20 ; j++) {
        t1 = MPI_Wtime();
        std::function<int(const surfingdb::table::mychunk &)> partitioner =
                [=](const surfingdb::table::mychunk &s) { return (s.a+j) % node->world; };
        t.shuffle(chunks2, partitioner);
        t2 = MPI_Wtime();
        if(node->rank == 0) {
            LOG(INFO) << j << "th shuffle " << vol * node->world << " on " << node->world << " workers costs "
                      << t2 - t1;
        }
    }
    // convert shuffled rows to columns
    surfingdb::table::ColumnarTable columnarTable;
    columnarTable.toTable(chunks2);
    chunks2.clear();
    return 0;
}
