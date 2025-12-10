/* JNI bridge for processors: expose shuffle via Arrow C Data Interface */

#include <jni.h>
#include <mpi.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>

#include <algorithm>
#include <memory>
#include <string>

#include "table/processors.h"

using matcha::table::processors;

static bool g_mpi_started = false;
static bool g_mpi_finalized = false;

static void ensure_mpi() {
  int initialized = 0;
  MPI_Initialized(&initialized);
  if (!initialized) {
    int provided = 0; int argc = 0; char** argv = nullptr;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  }
  g_mpi_started = true;
}

extern "C" JNIEXPORT jint JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_mpiRank(JNIEnv* env, jclass) {
  (void)env;
  ensure_mpi();
  int rank = 0; MPI_Comm_rank(MPI_COMM_WORLD, &rank); return rank;
}

extern "C" JNIEXPORT jint JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_mpiWorld(JNIEnv* env, jclass) {
  (void)env;
  ensure_mpi();
  int world = 1; MPI_Comm_size(MPI_COMM_WORLD, &world); return world;
}

extern "C" JNIEXPORT void JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_shuffle(
    JNIEnv* env, jclass,
    jlong schema_in_addr, jlong array_in_addr,
    jstring jfield_name, jboolean j_one_sided,
    jint j_rank, jint j_world,
    jlong schema_out_addr, jlong array_out_addr) {
  (void)env; // unused
  ensure_mpi();

  int actual_rank = 0;
  int actual_world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &actual_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &actual_world);

  int rank = actual_rank;
  int world = actual_world;
  if (j_world > 0) {
    if (actual_world > 1) {
      world = std::min<int>(j_world, actual_world);
      int requested_rank = j_rank;
      if (requested_rank >= 0 && requested_rank < actual_world) {
        rank = requested_rank;
      }
    } else {
      rank = 0;
      world = 1;
    }
  }

  auto* schema_in = reinterpret_cast<ArrowSchema*>(schema_in_addr);
  auto* array_in  = reinterpret_cast<ArrowArray*>(array_in_addr);

  auto rb_in = arrow::ImportRecordBatch(array_in, schema_in);
  if (!rb_in.ok()) {
    return;
  }
  auto batch = rb_in.MoveValueUnsafe();

  const char* c_field = env->GetStringUTFChars(jfield_name, nullptr);
  std::string field_name(c_field ? c_field : "");
  if (c_field) env->ReleaseStringUTFChars(jfield_name, c_field);

  // default partitioner: hash modulo world
  auto partitioner = [](size_t key_hash, int r, int w) { (void)r; return key_hash % w; };

  bool one_sided = (j_one_sided == JNI_TRUE);
  std::shared_ptr<arrow::RecordBatch> out;
  if (world <= 1) {
    out = batch;
  } else {
    out = processors::shuffle(batch, field_name, partitioner, one_sided, rank, world);
  }
  // export to output
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*out->schema().get(), schema_out);
  arrow::ExportRecordBatch(*out.get(), array_out, schema_out);
}

extern "C" JNIEXPORT void JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_cogroup(
    JNIEnv* env, jclass,
    jlong schema_in_left_addr, jlong array_in_left_addr,
    jlong schema_in_right_addr, jlong array_in_right_addr,
    jstring jfield_name, jboolean j_one_sided,
    jint j_rank, jint j_world,
    jlong schema_out_left_addr, jlong array_out_left_addr,
    jlong schema_out_right_addr, jlong array_out_right_addr) {
  (void)env;

  ensure_mpi();

  int actual_rank = 0;
  int actual_world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &actual_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &actual_world);

  int rank = actual_rank;
  int world = actual_world;
  if (j_world > 0) {
    if (actual_world > 1) {
      world = std::min<int>(j_world, actual_world);
      int requested_rank = j_rank;
      if (requested_rank >= 0 && requested_rank < actual_world) {
        rank = requested_rank;
      }
    } else {
      rank = 0;
      world = 1;
    }
  }

  auto* schema_left_in = reinterpret_cast<ArrowSchema*>(schema_in_left_addr);
  auto* array_left_in  = reinterpret_cast<ArrowArray*>(array_in_left_addr);
  auto* schema_right_in = reinterpret_cast<ArrowSchema*>(schema_in_right_addr);
  auto* array_right_in  = reinterpret_cast<ArrowArray*>(array_in_right_addr);

  std::shared_ptr<arrow::RecordBatch> left_rb;
  std::shared_ptr<arrow::RecordBatch> right_rb;
  if (schema_left_in && array_left_in) {
    auto maybe_left = arrow::ImportRecordBatch(array_left_in, schema_left_in);
    if (maybe_left.ok()) left_rb = maybe_left.MoveValueUnsafe();
  }
  if (schema_right_in && array_right_in) {
    auto maybe_right = arrow::ImportRecordBatch(array_right_in, schema_right_in);
    if (maybe_right.ok()) right_rb = maybe_right.MoveValueUnsafe();
  }

  const char* c_field = env->GetStringUTFChars(jfield_name, nullptr);
  std::string field_name(c_field ? c_field : "");
  if (c_field) env->ReleaseStringUTFChars(jfield_name, c_field);

  auto partitioner = [](size_t key_hash, int r, int w) { (void)r; return key_hash % w; };
  bool one_sided = (j_one_sided == JNI_TRUE);

  std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::RecordBatch>> result;
  if (world <= 1) {
    result = {left_rb, right_rb};
  } else {
    result = processors::cogroup(left_rb, right_rb, field_name, partitioner, one_sided, rank, world);
  }

  // export left
  if (result.first) {
    auto* schema_out_left = reinterpret_cast<ArrowSchema*>(schema_out_left_addr);
    auto* array_out_left  = reinterpret_cast<ArrowArray*>(array_out_left_addr);
    arrow::ExportSchema(*result.first->schema().get(), schema_out_left);
    arrow::ExportRecordBatch(*result.first.get(), array_out_left, schema_out_left);
  }
  // export right
  if (result.second) {
    auto* schema_out_right = reinterpret_cast<ArrowSchema*>(schema_out_right_addr);
    auto* array_out_right  = reinterpret_cast<ArrowArray*>(array_out_right_addr);
    arrow::ExportSchema(*result.second->schema().get(), schema_out_right);
    arrow::ExportRecordBatch(*result.second.get(), array_out_right, schema_out_right);
  }
}

extern "C" JNIEXPORT void JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_finalizeMPI(JNIEnv* env, jclass) {
  (void)env;
  if (!g_mpi_started || g_mpi_finalized) return;
  int initialized = 0;
  MPI_Initialized(&initialized);
  if (initialized) {
    // Ensure all ranks reach here
    MPI_Barrier(MPI_COMM_WORLD);
    int finalized = 0;
    MPI_Finalized(&finalized);
    if (!finalized) {
      MPI_Finalize();
    }
    g_mpi_finalized = true;
  }
}

extern "C" JNIEXPORT jstring JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_broadcastString(JNIEnv* env, jclass, jstring jmsg) {
  ensure_mpi();
  int rank = 0; MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  // Convert to bytes on rank 0
  std::string msg;
  if (rank == 0) {
    if (jmsg) {
      const char* c = env->GetStringUTFChars(jmsg, nullptr);
      if (c) { msg.assign(c); env->ReleaseStringUTFChars(jmsg, c); }
    }
  }
  // First broadcast length
  int32_t len = static_cast<int32_t>(msg.size());
  MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
  std::string recv(len, '\0');
  if (rank == 0) {
    if (len > 0) MPI_Bcast(msg.data(), len, MPI_CHAR, 0, MPI_COMM_WORLD);
  } else {
    if (len > 0) MPI_Bcast(recv.data(), len, MPI_CHAR, 0, MPI_COMM_WORLD);
  }
  const std::string& out = (rank == 0 ? msg : recv);
  return env->NewStringUTF(out.c_str());
}

extern "C" JNIEXPORT void JNICALL
Java_org_surfing_drsquirrel_jni_NativeProcessors_barrier(JNIEnv*, jclass) {
  ensure_mpi();
  MPI_Barrier(MPI_COMM_WORLD);
}
