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
#include <glog/logging.h>
#include <iostream>
#include "table/table.h"

using namespace surfingdb::table::schema;
using namespace surfingdb::table;

class mychunk {
public:
  int a;
  long b;
  char* ptr;
  mychunk() {
    ptr = static_cast<char*>(malloc(10));
  }
  static std::shared_ptr<arrow::Schema> schema() {
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("a", arrow::int32()),
      arrow::field("b", arrow::int64())
    };

    return std::make_shared<arrow::Schema>(schema_vector);
  }

  // friends defined inside class body are inline and are hidden from non-ADL lookup
  friend mychunk operator+(mychunk lhs,        // passing lhs by value helps optimize chained a+b+c
                           const mychunk& rhs) // otherwise, both parameters may be const references
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

  RowSchema r;
  r.fields = std::vector<surfingdb::table::schema::Field>();

  Field field1, field2, field3, field4, field5, field6, field7;

  initField(field1, "a", RowType::INT, sizeof(int));
  initField(field2, "b", RowType::LONG, sizeof(long));
  initField(field3, "c", RowType::BOOL, sizeof(bool));
  initField(field4, "d", RowType::DOUBLE, sizeof(double));
  initField(field5, "e", RowType::STRING, MAX_STR_LEN);

  initListField(field6, "l", RowType::DOUBLE, 3, sizeof(double));
  initMapField(field7, "m", RowType::STRING, RowType::LONG, 3, MAX_STR_LEN, sizeof(long));

  r.fields.push_back(field1);
  r.fields.push_back(field2);
  r.fields.push_back(field3);
  r.fields.push_back(field4);
  r.fields.push_back(field5);
  r.fields.push_back(field6);
  r.fields.push_back(field7);

  Value v1, v2, v3, v4, v5, v6, v7;
  v1.p_val.int_val = node->rank;

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

  // build continuous buffer with fixed fields offsets
  surfingdb::table::RowBuffer b(r);
  b.write(field1, v1);
  b.write(field2, v2);
  b.write(field3, v3);
  b.write(field4, v4);
  b.write(field5, v5);
  b.write(field6, v6);
  b.write(field7, v7);

  surfingdb::table::RowTable<surfingdb::table::RowBuffer> t_buf(node);
  t_buf.ingest(b);
  b.reg_row_type(r);
  LOG(INFO) << node->rank << &b;
  t_buf.send(0, 1, b, b.row_type());
  if (node->rank == 1) {
    LOG(INFO) << "offsets" << _offsets->at(field1);
    LOG(INFO) << "rank 1 recv " << b.size();
  }
  LOG(INFO) << node->rank << &b;
  //Value v;
  //b.read(field1, v);
  //LOG(INFO) << node->rank << " " << &v;
  MPI_Barrier(MPI_COMM_WORLD);
  return 0;
  // define a row table bind to each node
  surfingdb::table::RowTable<mychunk> t_l(node), t_r(node), t_out(node);
  //std::cout << "watermark is " << t.watermark() << std::endl;
  auto col = std::make_shared<surfingdb::table::ColumnarTable<mychunk>>();

  char* a = static_cast<char*>(malloc(1));
  t_l.send(0, 1, a, 1);

  mychunk c;
  t_l.regType(c.schema());

  c.b = (long)node->rank;
  c.a = node->rank;

  //t_l.send(0, 1, c);
  if (node->rank == 1) {
    std::cout << "rank 1 recv " << c.b << c.a << std::endl;
  }
  std::vector<mychunk> chunks;

  for (long i = 0; i < 10000000; i++) {
    chunks.push_back(c);
  }
  t_l.ingest(chunks);
  t_r.ingest(chunks);

  MPI_Barrier(MPI_COMM_WORLD);
  double t1, t2;
  t1 = MPI_Wtime();
  t_l.allreduce(chunks);

  t2 = MPI_Wtime();
  if (node->rank == 0) {
    LOG(INFO) << "all reduce cost " << t2 - t1;
  }
  t_l.flush(col);

  std::vector<mychunk> chunks2;
  int vol = 100000000 / node->world + 1;
  for (long i = 0; i < vol; i++) {
    c.a = i;
    chunks2.push_back(c);
  }
  t_l.ingest(chunks2);
  t1 = MPI_Wtime();

  // combine operator
  t_l.combine(t_r, t_out);

  // pardo operator
  std::function<void(const mychunk&, const mychunk&, int&)> doFunc =
    [=](const mychunk& l, const mychunk& r, int& out) { out = l.a + r.a; };
  surfingdb::table::ParDoOp<mychunk, mychunk, int> parDoOp(doFunc);
  surfingdb::table::RowTable<int> t_sum(node);
  t_l.parDo<mychunk, int>(t_l, t_sum, parDoOp);

  // partition operator
  std::function<int(const int&, const int&, const mychunk&)> ope =
    [=](const int& rank, const int& world, const mychunk& s) { return (s.a + rank) % world; };
  surfingdb::table::PartitionOp<mychunk> partitionOp(node->rank, node->world, t_l.row_type, ope);
  t_l.partition(partitionOp);

  t2 = MPI_Wtime();
  if (node->rank == 0) {
    LOG(INFO) << "shuffle " << vol * node->world << " on " << node->world << " workers costs "
              << t2 - t1;
  }

  t_l.flush(col);
  return 0;
}
