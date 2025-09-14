/*
 * MPI runner to validate processors::cogroup with nested Arrow schema on both sides.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>
#include <algorithm>

#include "table/processors.h"

using namespace matcha::table;

static std::shared_ptr<arrow::RecordBatch> make_nested_left(int rank, int world) {
  auto key_field = arrow::field("key", arrow::int64());
  auto list_field = arrow::field("lst", arrow::list(arrow::int32()));
  auto map_field = arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64()));
  auto val_field = arrow::field("lval", arrow::int32());
  auto schema = arrow::schema({key_field, list_field, map_field, val_field});

  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  arrow::ListBuilder list_builder(arrow::default_memory_pool(), std::make_shared<arrow::Int32Builder>());
  arrow::MapBuilder map_builder(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>());

  const int rows = 3;
  for (int i = 0; i < rows; ++i) {
    int64_t key = static_cast<int64_t>(rank) * 1000 + i; // keys: r*1000 + 0/1/2
    if (!key_builder.Append(key).ok()) return nullptr;
    if (!val_builder.Append(rank * 10 + i).ok()) return nullptr;

    if (!list_builder.Append().ok()) return nullptr;
    auto* li = static_cast<arrow::Int32Builder*>(list_builder.value_builder());
    for (int j = 0; j <= i; ++j) { if (!li->Append(j).ok()) return nullptr; }

    if (!map_builder.Append().ok()) return nullptr;
    auto* mk = static_cast<arrow::StringBuilder*>(map_builder.key_builder());
    auto* mv = static_cast<arrow::Int64Builder*>(map_builder.item_builder());
    if (!mk->Append("rank").ok() || !mv->Append(rank).ok()) return nullptr;
    if (!mk->Append("idx").ok() || !mv->Append(i).ok()) return nullptr;
    if (!mk->Append("world").ok() || !mv->Append(world).ok()) return nullptr;
    if (!mk->Append("side").ok() || !mv->Append(0).ok()) return nullptr; // left side marker
  }

  std::shared_ptr<arrow::Array> keys, vals, lsts, maps;
  if (!key_builder.Finish(&keys).ok()) return nullptr;
  if (!val_builder.Finish(&vals).ok()) return nullptr;
  if (!list_builder.Finish(&lsts).ok()) return nullptr;
  if (!map_builder.Finish(&maps).ok()) return nullptr;
  return arrow::RecordBatch::Make(schema, keys->length(), {keys, lsts, maps, vals});
}

static std::shared_ptr<arrow::RecordBatch> make_nested_right(int rank, int world) {
  auto key_field = arrow::field("key", arrow::int64());
  auto list_field = arrow::field("lst", arrow::list(arrow::int32()));
  auto map_field = arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64()));
  auto val_field = arrow::field("rval", arrow::int32());
  auto schema = arrow::schema({key_field, list_field, map_field, val_field});

  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  arrow::ListBuilder list_builder(arrow::default_memory_pool(), std::make_shared<arrow::Int32Builder>());
  arrow::MapBuilder map_builder(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>());

  const int rows = 3;
  for (int i = 0; i < rows; ++i) {
    int64_t key = static_cast<int64_t>(rank) * 1000 + (i + 1); // keys: r*1000 + 1/2/3
    if (!key_builder.Append(key).ok()) return nullptr;
    if (!val_builder.Append(rank * 100 + i).ok()) return nullptr;

    if (!list_builder.Append().ok()) return nullptr;
    auto* li = static_cast<arrow::Int32Builder*>(list_builder.value_builder());
    for (int j = 0; j <= i; ++j) { if (!li->Append(j).ok()) return nullptr; }

    if (!map_builder.Append().ok()) return nullptr;
    auto* mk = static_cast<arrow::StringBuilder*>(map_builder.key_builder());
    auto* mv = static_cast<arrow::Int64Builder*>(map_builder.item_builder());
    if (!mk->Append("rank").ok() || !mv->Append(rank).ok()) return nullptr;
    if (!mk->Append("idx").ok() || !mv->Append(i + 1).ok()) return nullptr;
    if (!mk->Append("world").ok() || !mv->Append(world).ok()) return nullptr;
    if (!mk->Append("side").ok() || !mv->Append(1).ok()) return nullptr; // right side marker
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

  auto L = make_nested_left(rank, world);
  auto R = make_nested_right(rank, world);
  if (!L || !R) { std::cerr << "Failed to build inputs" << std::endl; MPI_Abort(MPI_COMM_WORLD, 100); return 100; }

  auto out = processors::cogroup({L}, {R}, "key", partitioner, singleside, rank, world);
  auto Lp = out.first;
  auto Rp = out.second;

  auto validate_batch = [&](std::shared_ptr<arrow::RecordBatch> b, bool is_left) -> int {
    if (!b) return 0; // empty partition ok
    auto f_key = b->schema()->GetFieldByName("key");
    auto f_lst = b->schema()->GetFieldByName("lst");
    auto f_map = b->schema()->GetFieldByName("attrs");
    auto f_val = b->schema()->GetFieldByName(is_left ? "lval" : "rval");
    if (!f_key || !f_lst || !f_map || !f_val) return 1;
    if (f_key->type()->id() != arrow::Type::INT64 ||
        f_lst->type()->id() != arrow::Type::LIST ||
        f_map->type()->id() != arrow::Type::MAP ||
        f_val->type()->id() != arrow::Type::INT32) return 2;

    auto col_key = b->GetColumnByName("key");
    auto col_lst = b->GetColumnByName("lst");
    auto col_map = b->GetColumnByName("attrs");
    auto col_val = b->GetColumnByName(is_left ? "lval" : "rval");
    for (int64_t i = 0; i < b->num_rows(); ++i) {
      auto sk = col_key->GetScalar(i).ValueOrDie();
      if ((sk->hash() % world) != static_cast<size_t>(rank)) return 3;
      int64_t key = std::static_pointer_cast<arrow::Int64Scalar>(sk)->value;
      int origin_rank = static_cast<int>(key / 1000);
      int idx = static_cast<int>(key % 1000);

      auto sv = col_val->GetScalar(i).ValueOrDie();
      int32_t val = std::static_pointer_cast<arrow::Int32Scalar>(sv)->value;
      int32_t exp = is_left ? (origin_rank * 10 + idx) : (origin_rank * 100 + (idx - 1));
      if (val != exp) return 4;

      auto sl = col_lst->GetScalar(i).ValueOrDie();
      auto lvs = std::static_pointer_cast<arrow::ListScalar>(sl);
      if (!lvs->is_valid || !lvs->value) return 5;
      auto list_arr = std::static_pointer_cast<arrow::Int32Array>(lvs->value);
      int expected_len = is_left ? (idx + 1) : (idx); // right keys start at idx>=1, rows have len i+1 where i = idx-1
      if (list_arr->length() != expected_len) return 6;
      for (int32_t j = 0; j < list_arr->length(); ++j) if (list_arr->Value(j) != j) return 7;

      auto sm = col_map->GetScalar(i).ValueOrDie();
      auto mvs = std::static_pointer_cast<arrow::MapScalar>(sm);
      if (!mvs->is_valid || !mvs->value) return 8;
      auto struct_arr = std::static_pointer_cast<arrow::StructArray>(mvs->value);
      auto keys_arr = std::static_pointer_cast<arrow::StringArray>(struct_arr->field(0));
      auto vals_arr = std::static_pointer_cast<arrow::Int64Array>(struct_arr->field(1));
      bool ok_rank=false, ok_idx=false, ok_world=false, ok_side=false;
      for (int64_t j = 0; j < keys_arr->length(); ++j) {
        auto view = keys_arr->GetView(j);
        auto v = vals_arr->Value(j);
        if (view == std::string("rank") && v == origin_rank) ok_rank = true;
        if (view == std::string("idx") && v == idx) ok_idx = true;
        if (view == std::string("world") && v == world) ok_world = true;
        if (view == std::string("side") && v == (is_left ? 0 : 1)) ok_side = true;
      }
      if (!(ok_rank && ok_idx && ok_world && ok_side)) return 9;
    }
    return 0;
  };

  int errL = validate_batch(Lp, true);
  int errR = validate_batch(Rp, false);
  int err = std::max(errL, errR);
  int gerr = 0;
  MPI_Allreduce(&err, &gerr, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  if (gerr != 0) { MPI_Abort(MPI_COMM_WORLD, gerr); return gerr; }

  // Global row counts
  size_t local_rows_L = Lp ? Lp->num_rows() : 0;
  size_t local_rows_R = Rp ? Rp->num_rows() : 0;
  size_t global_rows_L = 0, global_rows_R = 0;
  MPI_Allreduce(&local_rows_L, &global_rows_L, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&local_rows_R, &global_rows_R, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  if (global_rows_L != static_cast<size_t>(3 * world) || global_rows_R != static_cast<size_t>(3 * world)) {
    MPI_Abort(MPI_COMM_WORLD, 10); return 10;
  }

  // Gather keys for global verification on each side
  auto gather_keys = [&](std::shared_ptr<arrow::RecordBatch> b) {
    std::vector<int64_t> local;
    if (b) {
      auto kc = b->GetColumnByName("key");
      for (int64_t i = 0; i < b->num_rows(); ++i) {
        auto s = kc->GetScalar(i).ValueOrDie();
        local.push_back(std::static_pointer_cast<arrow::Int64Scalar>(s)->value);
      }
    }
    int n = static_cast<int>(local.size());
    std::vector<int> counts(world, 0);
    MPI_Gather(&n, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    std::vector<int> displs; int total = 0;
    if (rank == 0) { displs.resize(world, 0); for (int i = 0; i < world; ++i) { displs[i] = total; total += counts[i]; } }
    std::vector<int64_t> all; if (rank == 0) all.resize(total);
    MPI_Gatherv(local.data(), n, MPI_LONG_LONG,
                rank == 0 ? all.data() : nullptr,
                rank == 0 ? counts.data() : nullptr,
                rank == 0 ? displs.data() : nullptr,
                MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    return all;
  };

  auto all_L = gather_keys(Lp);
  auto all_R = gather_keys(Rp);
  if (rank == 0) {
    std::vector<int64_t> expL, expR;
    expL.reserve(3 * world); expR.reserve(3 * world);
    for (int r = 0; r < world; ++r) { expL.push_back(static_cast<int64_t>(r) * 1000 + 0); expL.push_back(static_cast<int64_t>(r) * 1000 + 1); expL.push_back(static_cast<int64_t>(r) * 1000 + 2); }
    for (int r = 0; r < world; ++r) { expR.push_back(static_cast<int64_t>(r) * 1000 + 1); expR.push_back(static_cast<int64_t>(r) * 1000 + 2); expR.push_back(static_cast<int64_t>(r) * 1000 + 3); }
    std::sort(expL.begin(), expL.end()); std::sort(expR.begin(), expR.end());
    std::sort(all_L.begin(), all_L.end()); std::sort(all_R.begin(), all_R.end());
    if (expL != all_L) { MPI_Abort(MPI_COMM_WORLD, 11); return 11; }
    if (expR != all_R) { MPI_Abort(MPI_COMM_WORLD, 12); return 12; }
  }

  MPI_Finalize();
  return 0;
}

