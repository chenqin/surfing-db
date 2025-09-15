// Deeply nested, sophisticated schema for Arrow/Thrift conversion benchmarking

namespace java com.pinterest.deep.bench

struct Attr {
  1: string key,
  2: string val
}

struct Reading {
  1: i64 ts,
  2: double value,
  3: list<string> notes
}

struct Bundle {
  1: list<Reading> items,
  2: map<string, list<string>> extras
}

struct Region {
  1: string country,
  2: string city
}

struct Geo {
  1: double lat,
  2: double lon,
  3: Region region
}

struct Meta {
  1: map<string, string> labels,
  2: list<Attr> kvs
}

struct DeepEvent {
  1: i64 event_id,
  2: string source,
  3: list<list<i32>> metrics,
  4: set<i64> label_ids,
  5: map<string, list<i64>> counts_by_key,
  6: Meta meta,
  7: list<Reading> readings,
  8: map<string, Bundle> bundles,
  9: list<map<string, list<Attr>>> attr_maps,
  10: Geo geo
}

