namespace java org.surfing.mabs_metrics.thrift

struct MabsMetrics {
  1: required i64 timestamp,
  2: optional string service_tags,
  3: optional string node_tags,
  4: optional map<string, i64> counters,
  5: optional map<string, double> gauges,
  6: optional map<string, string> histograms,
  7: optional string service_name,
  8: optional map<string, double> double_counters
}

