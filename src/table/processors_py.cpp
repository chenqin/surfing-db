#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include <arrow/c/bridge.h>
#include <mpi.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>

#include "table/processors.h"

namespace py = pybind11;
using matcha::table::processors;

namespace {

bool g_mpi_started = false;
bool g_mpi_finalized = false;

void EnsureMPI() {
  int initialized = 0;
  MPI_Initialized(&initialized);
  if (!initialized) {
    int provided = 0;
    int argc = 0;
    char** argv = nullptr;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  }
  g_mpi_started = true;
}

void FinalizeMPIIfNeeded() {
  if (!g_mpi_started || g_mpi_finalized) {
    return;
  }
  int initialized = 0;
  MPI_Initialized(&initialized);
  if (!initialized) {
    return;
  }
  MPI_Barrier(MPI_COMM_WORLD);
  int finalized = 0;
  MPI_Finalized(&finalized);
  if (!finalized) {
    MPI_Finalize();
  }
  g_mpi_finalized = true;
}

std::shared_ptr<arrow::RecordBatch> ImportRecordBatch(const py::object& obj) {
  if (obj.is_none()) {
    return nullptr;
  }
  ArrowArray array{};
  ArrowSchema schema{};
  std::memset(&array, 0, sizeof(ArrowArray));
  std::memset(&schema, 0, sizeof(ArrowSchema));

  py::object exporter = obj.attr("_export_to_c");
  exporter(py::int_(reinterpret_cast<uintptr_t>(&array)),
           py::int_(reinterpret_cast<uintptr_t>(&schema)));

  auto maybe_batch = arrow::ImportRecordBatch(&array, &schema);
  if (!maybe_batch.ok()) {
    throw std::runtime_error(maybe_batch.status().ToString());
  }
  return maybe_batch.MoveValueUnsafe();
}

py::object ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (!batch) {
    return py::none();
  }
  auto schema_holder = std::make_unique<ArrowSchema>();
  auto array_holder = std::make_unique<ArrowArray>();
  std::memset(schema_holder.get(), 0, sizeof(ArrowSchema));
  std::memset(array_holder.get(), 0, sizeof(ArrowArray));

  auto status = arrow::ExportRecordBatch(*batch, array_holder.get(), schema_holder.get());
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }

  auto* schema_raw = schema_holder.release();
  auto* array_raw = array_holder.release();

  py::object pa = py::module::import("pyarrow");
  py::object rb_cls = pa.attr("RecordBatch");
  py::object importer = rb_cls.attr("_import_from_c");

  return importer(py::int_(reinterpret_cast<uintptr_t>(array_raw)),
                  py::int_(reinterpret_cast<uintptr_t>(schema_raw)));
}

std::tuple<int, int> ResolveRankWorld(std::optional<int> requested_rank,
                                      std::optional<int> requested_world) {
  int actual_rank = 0;
  int actual_world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &actual_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &actual_world);

  int use_rank = actual_rank;
  int use_world = actual_world;
  if (requested_world.has_value() && requested_world.value() > 0) {
    if (actual_world > 1) {
      use_world = std::min(requested_world.value(), actual_world);
      if (requested_rank.has_value() && requested_rank.value() >= 0 &&
          requested_rank.value() < actual_world) {
        use_rank = requested_rank.value();
      }
    } else {
      use_rank = 0;
      use_world = 1;
    }
  }
  return {use_rank, use_world};
}

py::object Shuffle(py::object batch_obj, const std::string& field_name,
                   bool one_sided, std::optional<int> rank,
                   std::optional<int> world) {
  EnsureMPI();
  auto [use_rank, use_world] = ResolveRankWorld(rank, world);

  auto batch = ImportRecordBatch(batch_obj);

  auto partitioner = [](size_t key_hash, int /*unused_rank*/, int world_size) {
    return static_cast<int>(key_hash % static_cast<size_t>(world_size));
  };

  std::shared_ptr<arrow::RecordBatch> out;
  if (use_world <= 1) {
    out = batch;
  } else {
    out = processors::shuffle(batch, field_name, partitioner, one_sided,
                              use_rank, use_world);
  }
  return ExportRecordBatch(out);
}

py::tuple Cogroup(py::object left_obj, py::object right_obj,
                  const std::string& field_name, bool one_sided,
                  std::optional<int> rank, std::optional<int> world) {
  EnsureMPI();
  auto [use_rank, use_world] = ResolveRankWorld(rank, world);

  auto left_batch = ImportRecordBatch(left_obj);
  auto right_batch = ImportRecordBatch(right_obj);

  auto partitioner = [](size_t key_hash, int /*unused_rank*/, int world_size) {
    return static_cast<int>(key_hash % static_cast<size_t>(world_size));
  };

  std::pair<std::shared_ptr<arrow::RecordBatch>,
            std::shared_ptr<arrow::RecordBatch>> result;

  if (use_world <= 1) {
    result = {left_batch, right_batch};
  } else {
    result = processors::cogroup(left_batch, right_batch, field_name,
                                 partitioner, one_sided, use_rank,
                                 use_world);
  }

  return py::make_tuple(ExportRecordBatch(result.first),
                        ExportRecordBatch(result.second));
}

int MPIRank() {
  EnsureMPI();
  int rank = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  return rank;
}

int MPIWorld() {
  EnsureMPI();
  int world = 1;
  MPI_Comm_size(MPI_COMM_WORLD, &world);
  return world;
}

std::string BroadcastString(const std::optional<std::string>& message) {
  EnsureMPI();
  int rank = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  std::string msg;
  if (rank == 0) {
    if (!message.has_value()) {
      throw std::invalid_argument(
          "Rank 0 must provide a message for broadcast_string");
    }
    msg = message.value();
  } else if (message.has_value()) {
    msg = message.value();
  }

  int32_t len = static_cast<int32_t>(msg.size());
  MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);

  std::string buffer(len, '\0');
  if (rank == 0) {
    if (len > 0) {
      MPI_Bcast(msg.data(), len, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
  } else {
    if (len > 0) {
      MPI_Bcast(buffer.data(), len, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
  }
  return rank == 0 ? msg : buffer;
}

void Barrier() {
  EnsureMPI();
  MPI_Barrier(MPI_COMM_WORLD);
}

}  // namespace

PYBIND11_MODULE(surfingprocessorspy, m) {
  m.doc() = "Python bindings for SurfingDB MPI processors";

  m.def("mpi_rank", &MPIRank, "Return the MPI rank (initialising MPI if needed)");
  m.def("mpi_world", &MPIWorld,
        "Return the MPI world size (initialising MPI if needed)");

  m.def("shuffle", &Shuffle,
        py::arg("record_batch"),
        py::arg("field_name"),
        py::arg("one_sided") = true,
        py::arg("rank") = py::none(),
        py::arg("world") = py::none(),
        "Shuffle a RecordBatch across ranks using Arrow C Data Interface");

  m.def("cogroup", &Cogroup,
        py::arg("left"),
        py::arg("right"),
        py::arg("field_name"),
        py::arg("one_sided") = true,
        py::arg("rank") = py::none(),
        py::arg("world") = py::none(),
        "Cogroup two RecordBatches across ranks, returning a tuple of batches");

  m.def("broadcast_string", &BroadcastString,
        py::arg("message") = py::none(),
        "Broadcast a UTF-8 string from rank 0 to all ranks");

  m.def("barrier", &Barrier, "Synchronise all ranks via MPI_Barrier");

  m.def("finalize_mpi", &FinalizeMPIIfNeeded,
        "Finalize MPI across all ranks if initialised by these bindings");
}
