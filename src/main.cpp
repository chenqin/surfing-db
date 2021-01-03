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
    c.reg();

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
    t.allreduce(chunks, t.sum);
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();
    LOG(INFO) << node->rank << " " << t2 - t1;
    std::vector<surfingdb::table::mychunk> chunks2;

    for(long i = 0 ; i < 1000000 ; i++) {
        c.a = i;
        chunks2.push_back(c);
    }
    t.shuffle(chunks2);
    LOG(INFO) << node->rank << " " << chunks2.size();
    return 0;
}
