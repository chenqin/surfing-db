java_library(
  name = "utils-mabs",
  srcs = glob(["*.java"]),

  deps = [
        "@commons_collections",
        "@com_fasterxml_jackson_core_jackson_annotations//:com_fasterxml_jackson_core_jackson_annotations",
        "@org_slf4j_api",
         "@com_tdunning_t_digest",
        "@org_apache_flink_core_1_11_20211117",
        "@org_apache_flink_streaming_java_2_11_1_11_20211117//:org_apache_flink_streaming_java_2_11_1_11_20211117",
        "@schemas//:schemas_thrift_mabs_metrics",
        "@org_apache_flink_runtime_2_11_1_11_20211117//:org_apache_flink_runtime_2_11_1_11_20211117",
        "//xenon/src/main/java/com/pinterest/xenon/commons:xenon-commons-java_1_11_20211117",
        "//xenon/src/main/scala/com/pinterest/xenon/commons:xenon-commons_1_11_20211117",
        "//xenon/src/main/scala/com/pinterest/xenon/flink:xenon-flink_1_11_20211117",
        "@schemas//:schemas_thrift_text_message",
    ],
  visibility = ["//visibility:public"],
)