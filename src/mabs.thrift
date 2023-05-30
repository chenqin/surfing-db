// Thrift structure for sending MABS
// (Metrics Aggregation by Service) messages

namespace java com.pinterest.mabs_metrics.thrift

struct MabsMetrics {

  // Time of metric generation in epoch time
  1: required i64 timestamp,

  // Tagstring containing service level info. (i.e nimbus_uuid, host_type...)
  2: optional string service_tags,

  // Tagstring containing node level info. (i.e host, pod)
  3: optional string node_tags,

  // Store the counters.
  4: optional map<string, i64> counters,

  // Store the gauges.
  5: optional map<string, double> gauges,

  // Store the histograms.
  6: optional map<string, string> histograms,

  // service name.
  7: optional string service_name,

  // Store the double counters.
  8: optional map<string, double> double_counters,

}
