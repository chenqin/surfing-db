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

#include <glog/logging.h>

#include <chrono>
#include <future>
#include <iostream>
#include <string>

#include "connector/kafka.h"
#include "meta/node.h"
#include "table/processors.h"
#include "table/utils.h"

using namespace matcha::table::schema;
using matcha::meta::node;
using namespace matcha::table;
using namespace matcha::connector;
using namespace std::chrono;
namespace cp = ::arrow::compute;

volatile std::sig_atomic_t terminal_signal;
void signal_handler(int signal) {
  std::cout << "user exit program";
  terminal_signal = signal;
}
/**
 * @brief  run local flink cluster with with mpi launcher
 * mpirun -np 2 LocalCluster --configDir /home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/conf -D taskmanager.memory.network.min=134217730b -D taskmanager.cpu.cores=1.0 -D taskmanager.memory.task.off-heap.size=0b -D taskmanager.memory.jvm-metaspace.size=268435456b -D external-resources=none -D taskmanager.memory.jvm-overhead.min=201326592b -D taskmanager.memory.framework.off-heap.size=134217728b -D taskmanager.memory.network.max=134217730b -D taskmanager.memory.framework.heap.size=134217728b -D taskmanager.memory.managed.size=536870920b -D taskmanager.memory.task.heap.size=402653174b -D taskmanager.numberOfTaskSlots=1 -D taskmanager.memory.jvm-overhead.max=201326592b -D jobmanager.memory.off-heap.size=134217728b -D jobmanager.memory.jvm-overhead.min=201326592b -D jobmanager.memory.jvm-metaspace.size=268435456b -D jobmanager.memory.heap.size=1073741824b -D jobmanager.memory.jvm-overhead.max=201326592b --executionMode cluster
 *
 * @param argc
 * @param argv
 * @return int
 */

int main(int argc, char* argv[]) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);
  //  create node of cluster
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<matcha::meta::node>(&argc, &argv);
  std::signal(SIGTERM | SIGINT, signal_handler);

  JavaVMOption options[10];

  options[0].optionString = "-XX:+UseG1GC";
  options[1].optionString = "-Xmx536870902";
  options[2].optionString = "-Xms5370902";
  options[3].optionString = "-XX:MaxDirectMemorySize=268435458";
  options[4].optionString = "-XX:MaxMetaspaceSize=268435456";
  options[5].optionString = "-Dlog.file=/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/log/flink-chen-taskexecutor-0-chenqin.log";
  options[6].optionString = "-Dlog4j.configuration=file:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/conf/log4j.properties";
  options[7].optionString = "-Dlog4j.configurationFile=file:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/conf/log4j.properties";
  options[8].optionString = "-Dlogback.configurationFile=file:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/conf/logback.xml";
  options[9].optionString = "-Djava.class.path=/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-cep-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-connector-files-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-csv-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-json-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-scala_2.12-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-table-api-java-uber-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-table-planner-loader-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-table-runtime-1.18-SNAPSHOT.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/log4j-1.2-api-2.17.1.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/log4j-api-2.17.1.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/log4j-core-2.17.1.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/log4j-slf4j-impl-2.17.1.jar:/home/chen/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/flink-dist-1.18-SNAPSHOT.jar::::";

  JavaVMInitArgs vm_args;
  vm_args.version = JNI_VERSION_1_8;
  vm_args.nOptions = 10;
  vm_args.options = options;
  vm_args.ignoreUnrecognized = JNI_TRUE;

  JNIEnv* env;
  JavaVM* jvm;
  int status = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
  if (status < 0) {
    std::cerr << "\n<<<<< Unable to Launch JVM >>>>>\n"
              << std::endl;
    env = nullptr;
  }
  CHECK_NOTNULL(env);
  jclass cls;
  if (node->rank == 0) {
    cls = env->FindClass("org/apache/flink/runtime/entrypoint/StandaloneSessionClusterEntrypoint");
  } else {
    cls = env->FindClass("org/apache/flink/runtime/taskexecutor/TaskManagerRunner");
  }
  CHECK(cls != nullptr);

  // Locate the static method
  jmethodID mid = env->GetStaticMethodID(cls, "main", "([Ljava/lang/String;)V");
  CHECK(mid != nullptr);

  // Construct the arguments to the method
  // Assume `argc` is the number of arguments and `argv` is an array of C-string arguments
  CHECK(argc > 1); // skip executable name
  jobjectArray args = env->NewObjectArray(argc - 1, env->FindClass("java/lang/String"), nullptr);
  for (int i = 0; i < argc - 1; ++i) {
    // std::cout << argv[i] << std::endl;
    env->SetObjectArrayElement(args, i, env->NewStringUTF(argv[i + 1]));
  }

  // Call the static method
  env->CallStaticVoidMethod(cls, mid, args);
  if (env->ExceptionCheck()) {
    // An exception occurred while calling the method
    env->ExceptionDescribe(); // Prints the stack trace of the exception
    env->ExceptionClear();
    // Now you can either return, terminate the program, or continue executing
    // other code - the exception has been handled.
  }
  return terminal_signal;
}
