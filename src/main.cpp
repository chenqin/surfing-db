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
#include <iostream>

/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main() {
    // create node of cluster
    const auto node = std::make_shared<surfingdb::table::Node>();
    // define a row table bind to each node
    surfingdb::table::RowTable t(node);
    std::cout << "watermark is " << t.watermark() << std::endl;
    return 0;
}
