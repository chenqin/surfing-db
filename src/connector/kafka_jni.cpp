/* JNI bridge for KafkaConnector -> Java via Arrow C Data Interface */

#include <jni.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>

#include <memory>
#include <string>
#include <vector>

#include "connector/kafka.h"
#include "table/utils.h"
#include "meta/node.h"

using matcha::connector::KafkaConnector;
using matcha::meta::node;
using matcha::meta::schema::RowSchema;
using matcha::meta::schema::RowType;
using matcha::meta::SchemaUtils;
using matcha::table::mschema;
using matcha::table::utils;

namespace {
struct KafkaJNIHandle {
  std::shared_ptr<node> node_ptr;
  std::shared_ptr<mschema> schema_ptr;
  std::unique_ptr<KafkaConnector> connector;
};

static std::vector<std::string> JArrayToStringVector(JNIEnv* env, jobjectArray arr) {
  std::vector<std::string> out;
  if (!arr) return out;
  jsize n = env->GetArrayLength(arr);
  out.reserve(n);
  for (jsize i = 0; i < n; ++i) {
    jstring jstr = (jstring)env->GetObjectArrayElement(arr, i);
    const char* cstr = env->GetStringUTFChars(jstr, nullptr);
    out.emplace_back(cstr);
    env->ReleaseStringUTFChars(jstr, cstr);
    env->DeleteLocalRef(jstr);
  }
  return out;
}
} // namespace

extern "C" JNIEXPORT jlong JNICALL
Java_org_surfing_drsquirrel_jni_NativeKafkaConnector_create(
    JNIEnv* env, jclass, jstring jname, jint jbatch, jint jinterval,
    jobjectArray jtopics, jstring jserverset, jstring jgroupid, jboolean jpii) {
  (void)jname; // unused
  const char* serverset = env->GetStringUTFChars(jserverset, nullptr);
  const char* groupid = env->GetStringUTFChars(jgroupid, nullptr);
  auto topics = JArrayToStringVector(env, jtopics);

  // Build schema: topic (string), payload (string)
  RowSchema r;
  SchemaUtils::initField(r, "topic", RowType::STRING, 52400);
  SchemaUtils::initField(r, "payload", RowType::STRING, 52400);
  auto schema_ptr = std::make_shared<mschema>(r);

  // minimal argv to init MPI via node
  int argc = 1; char arg0[] = "KafkaJNI"; char* argvv[] = {arg0, nullptr}; char** argvp = argvv;
  auto node_ptr = std::make_shared<node>(&argc, &argvp, std::string(""));

  // batch and interval
  int batch = static_cast<int>(jbatch);
  int interval = static_cast<int>(jinterval);

  auto handle = new KafkaJNIHandle();
  handle->node_ptr = node_ptr;
  handle->schema_ptr = schema_ptr;
  handle->connector = std::make_unique<KafkaConnector>(
      node_ptr, "kafka-jni", batch, interval, schema_ptr, topics,
      std::string(serverset), std::string(groupid), jpii == JNI_TRUE);

  env->ReleaseStringUTFChars(jserverset, serverset);
  env->ReleaseStringUTFChars(jgroupid, groupid);
  return reinterpret_cast<jlong>(handle);
}

extern "C" JNIEXPORT void JNICALL
Java_org_surfing_drsquirrel_jni_NativeKafkaConnector_destroy(
    JNIEnv*, jclass, jlong jhandle) {
  auto* handle = reinterpret_cast<KafkaJNIHandle*>(jhandle);
  if (!handle) return;
  delete handle; // unique_ptr members clean up
}

extern "C" JNIEXPORT void JNICALL
Java_org_surfing_drsquirrel_jni_NativeKafkaConnector_pollOnce(
    JNIEnv* env, jclass, jlong jhandle, jlong schema_out_addr, jlong array_out_addr) {
  auto* h = reinterpret_cast<KafkaJNIHandle*>(jhandle);
  if (!h) return;
  // consume batch into Arrow RB with default deser (topic/payload strings)
  auto batch = h->connector->consume_batch([
      &h](const void* payload, size_t len,
           std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) -> size_t {
    matcha::table::schema::PValue v1, v2; matcha::table::schema::Value placeholder;
    v1.string_val = "kafka";
    v2.string_val = std::string(reinterpret_cast<const char*>(payload), len);
    utils::append(builders.at(0).get(), h->schema_ptr->fields.at(0), v1, placeholder);
    utils::append(builders.at(1).get(), h->schema_ptr->fields.at(1), v2, placeholder);
    return 1;
  });

  // export via C Data Interface
  auto* out_schema = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* out_array  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*batch->schema().get(), out_schema);
  arrow::ExportRecordBatch(*batch.get(), out_array, out_schema);
}
