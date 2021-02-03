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
#include <omp.h>
#include "table/table.h"

using namespace surfingdb::table::schema;
using namespace surfingdb::table;

/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main(int argc, char** argv) {
  LOG(INFO) << "total omp threads # " << omp_get_max_threads();
  omp_set_num_threads(omp_get_max_threads());
  //google::InitGoogleLogging(argv[0]);
  // create node of cluster
  const auto node = std::make_shared<surfingdb::table::Node>(&argc, &argv);

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

  /**
  * ingestion
  */
  if (node->rank == 0) {
    size_t i;
    for (i = 0; i < 3000; i++) {
      tsed.ingest(b);
    }
  }

  node->forward(); // stage 1 broadcast data

  // stage 2 run k_mean
  std::vector<Field> fields;
  fields.push_back(field4);
  fields.resize(1);

  KMeanOperator op(1, fields, 100);
  tsed.process(op);

  // stage 3, run xgb
  tsed.ingest(b);
  LOG(INFO) << "xgb operator"
            << " process " << node->rank << " has " << tsed.count() << " rows";
  XGBParameters parameters;
  parameters.tree_method = "exact";
  parameters.objective = "reg:linear";
  parameters.min_child_weight = 1;
  parameters.gamma = 0.1;
  parameters.max_depth = 2;
  parameters.verbosity = true;
  parameters.eval_metric = "error";
  XGBOperator xgbOperator(fields, field4, parameters, node->rank, node->world);
  tsed.process(xgbOperator);

  xgbOperator.parameters.isTraining = false; // now switch to prediction
  tsed.process(xgbOperator);

  tsed.group(field1);
  tsed.clear(); //release resources

  Value v;
  v.p_val.int_val = 1;
  b.write(field1, v);
  TempTable t1(node, schema_ptr);
  TempTable t2(node, schema_ptr);
  for (int i = 0; i < 3000; i++) {
    v.p_val.int_val = i + node->rank;
    b.write(field1, v);
    t1.ingest(b);
    t2.ingest(b);
  }
  int itr = 0;
  while(itr++ < 100) {
    TempTable tout1(node, schema_ptr);
    TempTable tout2(node, schema_ptr);
    t1.shuffle(field1, tout1);
    t2.shuffle(field1, tout2);
    tout1.verify(field1);
    tout2.verify(field1);
  }
  // tout1 and tout2 shared with same key to each process, per key co_group is straight forward
  return 0;
}
