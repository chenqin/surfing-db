/* JNI bridge for processors: expose shuffle via Arrow C Data Interface */

#include <jni.h>
#include <mpi.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>

#include <memory>
#include <string>

#include "table/processors.h"

using matcha::table::processors;

extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeProcessors_shuffle(
    JNIEnv* env, jclass,
    jlong schema_in_addr, jlong array_in_addr,
    jstring jfield_name, jboolean j_one_sided,
    jint j_rank, jint j_world,
    jlong schema_out_addr, jlong array_out_addr) {
  (void)env; // unused

  int initialized = 0;
  MPI_Initialized(&initialized);
  if (!initialized) {
    int provided = 0;
    int argc = 0; char** argv = nullptr;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  }
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  // If caller provided rank/world, ensure they match the actual MPI world (best-effort)
  (void)j_rank; (void)j_world;

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
  auto out = processors::shuffle(batch, field_name, partitioner, one_sided, rank, world);

  // export to output
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*out->schema().get(), schema_out);
  arrow::ExportRecordBatch(*out.get(), array_out, schema_out);
}

