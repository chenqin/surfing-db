/*
 * MPI runner to validate shuffle with nested Arrow schema (list, map) across ranks.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>
#include <algorithm>

#include "table/processors.h"

using namespace matcha::table;

static std::shared_ptr<arrow::RecordBatch> make_nested_batch_for_rank(int rank, int world) {
  // Schema: key:int64, lst:list<int32>, attrs: map<string,int64>, val:int32
  auto key_field = arrow::field("key", arrow::int64());
  auto list_field = arrow::field("lst", arrow::list(arrow::int32()));
  auto map_field = arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64()));
  auto val_field = arrow::field("val", arrow::int32());
  auto schema = arrow::schema({key_field, list_field, map_field, val_field});

  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  arrow::ListBuilder list_builder(arrow::default_memory_pool(), std::make_shared<arrow::Int32Builder>());
  arrow::MapBuilder map_builder(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>());

  // Build 3 rows per rank with deterministic content derived from rank and idx
  const int rows = 3;
  for (int i = 0; i < rows; ++i) {
    int64_t key = static_cast<int64_t>(rank) * 1000 + i; // encode origin in key
    if (!key_builder.Append(key).ok()) return nullptr;
    if (!val_builder.Append(rank * 10 + i).ok()) return nullptr;

    // list column: variable length per row, values [0, 1, ..., i]
    if (!list_builder.Append().ok()) return nullptr;
    auto* li = static_cast<arrow::Int32Builder*>(list_builder.value_builder());
    for (int j = 0; j <= i; ++j) {
      if (!li->Append(j).ok()) return nullptr;
    }

    // map column: {"rank": rank, "idx": i, "world": world}
    if (!map_builder.Append().ok()) return nullptr;
    auto* mk = static_cast<arrow::StringBuilder*>(map_builder.key_builder());
    auto* mv = static_cast<arrow::Int64Builder*>(map_builder.item_builder());
    if (!mk->Append("rank").ok() || !mv->Append(rank).ok()) return nullptr;
    if (!mk->Append("idx").ok() || !mv->Append(i).ok()) return nullptr;
    if (!mk->Append("world").ok() || !mv->Append(world).ok()) return nullptr;
  }

  std::shared_ptr<arrow::Array> keys, vals, lsts, maps;
  if (!key_builder.Finish(&keys).ok()) return nullptr;
  if (!val_builder.Finish(&vals).ok()) return nullptr;
  if (!list_builder.Finish(&lsts).ok()) return nullptr;
  if (!map_builder.Finish(&maps).ok()) return nullptr;

  return arrow::RecordBatch::Make(schema, keys->length(), {keys, lsts, maps, vals});
}

static size_t partitioner(size_t key_hash, int /*r*/, int w) { return key_hash % w; }

int main(int argc, char** argv) {
  int provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  bool singleside = true;
  if (argc > 1 && std::string(argv[1]) == std::string("two")) singleside = false;

  auto local = make_nested_batch_for_rank(rank, world);
  if (!local) {
    std::cerr << "Failed to build local batch" << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 1);
    return 1;
  }

  auto shuffled = processors::shuffle(local, "key", partitioner, singleside, rank, world);
  if (!shuffled) {
    std::cerr << "Shuffle returned null" << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 2);
    return 2;
  }

  // Validate schema types
  auto f_key = shuffled->schema()->GetFieldByName("key");
  auto f_lst = shuffled->schema()->GetFieldByName("lst");
  auto f_map = shuffled->schema()->GetFieldByName("attrs");
  auto f_val = shuffled->schema()->GetFieldByName("val");
  if (!f_key || !f_lst || !f_map || !f_val) {
    std::cerr << "Missing expected fields on rank " << rank << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 3);
    return 3;
  }
  if (f_key->type()->id() != arrow::Type::INT64 ||
      f_lst->type()->id() != arrow::Type::LIST ||
      f_map->type()->id() != arrow::Type::MAP ||
      f_val->type()->id() != arrow::Type::INT32) {
    std::cerr << "Unexpected field types on rank " << rank << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 4);
    return 4;
  }

  // Validate partition rule and nested content derived from key
  auto col_key = shuffled->GetColumnByName("key");
  auto col_lst = shuffled->GetColumnByName("lst");
  auto col_map = shuffled->GetColumnByName("attrs");
  auto col_val = shuffled->GetColumnByName("val");

  for (int64_t i = 0; i < shuffled->num_rows(); ++i) {
    auto sk = col_key->GetScalar(i).ValueOrDie();
    if ((sk->hash() % world) != static_cast<size_t>(rank)) {
      std::cerr << "Partition validation failed on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 5);
      return 5;
    }
    // Decode origin from key
    int64_t key = std::static_pointer_cast<arrow::Int64Scalar>(sk)->value;
    int origin_rank = static_cast<int>(key / 1000);
    int idx = static_cast<int>(key % 1000);

    // Check val matches origin_rank*10+idx
    auto sv = col_val->GetScalar(i).ValueOrDie();
    int32_t val = std::static_pointer_cast<arrow::Int32Scalar>(sv)->value;
    if (val != origin_rank * 10 + idx) {
      std::cerr << "val mismatch on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 6);
      return 6;
    }

    // Check list equals [0, 1, ..., idx] (variable length)
    auto sl = col_lst->GetScalar(i).ValueOrDie();
    auto lvs = std::static_pointer_cast<arrow::ListScalar>(sl);
    if (!lvs->is_valid || !lvs->value) { std::cerr << "list null" << std::endl; MPI_Abort(MPI_COMM_WORLD, 7); return 7; }
    auto list_arr = std::static_pointer_cast<arrow::Int32Array>(lvs->value);
    if (list_arr->length() != (idx + 1)) {
      std::cerr << "list length mismatch on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 8);
      return 8;
    }
    for (int32_t j = 0; j < list_arr->length(); ++j) {
      if (list_arr->Value(j) != j) {
        std::cerr << "list content mismatch on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 8);
        return 8;
      }
    }

    // Check map contains expected entries
    auto sm = col_map->GetScalar(i).ValueOrDie();
    auto mvs = std::static_pointer_cast<arrow::MapScalar>(sm);
    if (!mvs->is_valid || !mvs->value) { std::cerr << "map null" << std::endl; MPI_Abort(MPI_COMM_WORLD, 9); return 9; }
    // MapScalar holds a StructArray of (key,item) pairs
    auto struct_arr = std::static_pointer_cast<arrow::StructArray>(mvs->value);
    auto keys_arr = std::static_pointer_cast<arrow::StringArray>(struct_arr->field(0));
    auto vals_arr = std::static_pointer_cast<arrow::Int64Array>(struct_arr->field(1));
    // Look for required keys
    bool ok_rank=false, ok_idx=false, ok_world=false;
    for (int64_t j = 0; j < keys_arr->length(); ++j) {
      auto view = keys_arr->GetView(j);
      auto v = vals_arr->Value(j);
      if (view == std::string("rank") && v == origin_rank) ok_rank = true;
      if (view == std::string("idx") && v == idx) ok_idx = true;
      if (view == std::string("world") && v == world) ok_world = true;
    }
    if (!(ok_rank && ok_idx && ok_world)) {
      std::cerr << "map content mismatch on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 10);
      return 10;
    }
  }

  // Check global row count equals rows_per_rank * world
  size_t local_rows = shuffled->num_rows();
  size_t global_rows = 0;
  MPI_Allreduce(&local_rows, &global_rows, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  if (global_rows != static_cast<size_t>(3 * world)) {
    std::cerr << "Global row count mismatch: " << global_rows << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 11);
    return 11;
  }

  // Global verification: gather all keys and compare with expected set
  std::vector<int64_t> local_keys;
  local_keys.reserve(shuffled->num_rows());
  for (int64_t i = 0; i < shuffled->num_rows(); ++i) {
    auto sk = col_key->GetScalar(i).ValueOrDie();
    int64_t key = std::static_pointer_cast<arrow::Int64Scalar>(sk)->value;
    local_keys.push_back(key);
  }

  int local_n = static_cast<int>(local_keys.size());
  std::vector<int> counts(world, 0);
  MPI_Gather(&local_n, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);

  std::vector<int> displs;
  int total = 0;
  if (rank == 0) {
    displs.resize(world, 0);
    for (int i = 0; i < world; ++i) {
      displs[i] = total;
      total += counts[i];
    }
  }

  std::vector<int64_t> all_keys;
  if (rank == 0) all_keys.resize(total);
  MPI_Gatherv(local_keys.data(), local_n, MPI_LONG_LONG,
              rank == 0 ? all_keys.data() : nullptr,
              rank == 0 ? counts.data() : nullptr,
              rank == 0 ? displs.data() : nullptr,
              MPI_LONG_LONG, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    // Expected keys: for each origin rank r, r*1000 + {0,1,2}
    std::vector<int64_t> expected;
    expected.reserve(3 * world);
    for (int r = 0; r < world; ++r) {
      expected.push_back(static_cast<int64_t>(r) * 1000 + 0);
      expected.push_back(static_cast<int64_t>(r) * 1000 + 1);
      expected.push_back(static_cast<int64_t>(r) * 1000 + 2);
    }
    std::sort(expected.begin(), expected.end());
    std::sort(all_keys.begin(), all_keys.end());
    if (expected != all_keys) {
      std::cerr << "Global key set mismatch after shuffle\n";
      MPI_Abort(MPI_COMM_WORLD, 12);
      return 12;
    }
  }

  MPI_Finalize();
  return 0;
}
