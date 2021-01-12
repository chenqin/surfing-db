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

#include <future>
#include <glog/logging.h>
#include <iostream>
#include "table/table.h"

using namespace surfingdb::table::schema;
using namespace surfingdb::table;

/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main() {
  auto num = omp_get_thread_num();
  LOG(INFO) << "threads # " << num;
  //google::InitGoogleLogging(argv[0]);
  // create node of cluster
  const auto node = std::make_shared<surfingdb::table::Node>();

  RowSchema r;
  r.fields = std::vector<surfingdb::table::schema::Field>();

  Field field1, field2, field3, field4, field5, field6, field7;

  initField(field1, "a", RowType::INT, sizeof(int));
  initField(field2, "b", RowType::LONG, sizeof(long));
  initField(field3, "c", RowType::BOOL, sizeof(bool));
  initField(field4, "d", RowType::DOUBLE, sizeof(double));
  initField(field5, "e", RowType::STRING, MAX_STR_LEN);

  initListField(field6, "l", RowType::DOUBLE, 2, sizeof(double));
  initMapField(field7, "m", RowType::STRING, RowType::LONG, 3, MAX_STR_LEN, sizeof(long));

  r.fields.push_back(field1);
  r.fields.push_back(field2);
  r.fields.push_back(field3);
  r.fields.push_back(field4);
  r.fields.push_back(field5);
  r.fields.push_back(field6);
  r.fields.push_back(field7);

  std::shared_ptr<TableSchema> schema_ptr = std::make_shared<TableSchema>(r);

  Value v1, v2, v3, v4, v5, v6, v7;
  v1.p_val.int_val = node->rank + 1;

  v2.p_val.long_val = 4;

  v3.p_val.bool_val = true;

  v4.p_val.double_val = 0.1f;

  v5.p_val.string_val = "hello";
  PValue p;
  p.double_val = 0.1;
  std::vector<PValue> lval;
  lval.push_back(p);
  v6.list_value = lval;
  PValue key, value;
  key.string_val = "hello";
  value.long_val = 1l;
  std::pair<PValue, PValue> pair;
  pair.first = key;
  pair.second = value;
  v7.map_value.insert(pair);

  surfingdb::table::RowBuffer b(schema_ptr);
  b.write(field1, v1);
  b.write(field2, v2);
  b.write(field3, v3);
  b.write(field4, v4);
  b.write(field5, v5);
  b.write(field6, v6);
  b.write(field7, v7);

  /**
   * ingestion
   */
  TempTable tsed(node, schema_ptr);

  int iteration = 0;
  while (iteration++ < 10) {
    /**
  * ingestion
  */
    if (node->rank == 0) {
      size_t i;
      for (i = 0; i < 3000; i++) {
        tsed.ingest(b);
      }
    }
    node->forward();
    /**
   * broadcast, recve
   */
    if (node->rank == 0) {
      for (int j = 1; j < node->world; j++) {
        MPI_Request req;
        tsed.async_send(j, 3000, req);
        node->keep(std::make_unique<MPI_Request>(req));
      }
    } else {
      TempTable trecv(node, schema_ptr);
      MPI_Request request;
      trecv.async_recv(0, request);
      auto revcallback = std::async(std::launch::deferred, [&request, &trecv, field1]() {
        MPI_Status status;
        MPI_Wait(&request, &status);
        trecv.complete();
        // after recv all items, loop over and modify in parallel parDo
#pragma omp parallel for
        for(int i = 0 ; i < 3000 ; i++) {
          auto s = trecv.read(i);
          Value v, vm;
          s->read(field1, v);
          v.p_val.int_val = 2;
          s->write(field1, v);
          s->read(field1, vm);
        }
      });

      // do something not waiting for sent data here
      revcallback.wait();
    }
    node->forward(); // stage 1 broadcast data
  }
  return 0;
}
