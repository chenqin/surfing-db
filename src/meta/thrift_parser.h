// Simple .thrift IDL parser to convert a struct into mschema/Arrow schema
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <unordered_map>
#include "meta/schema.h"

namespace matcha {
namespace meta {

struct ThriftParseOptions {
  uint64_t default_string_max = 1024;      // bytes per string
  uint64_t default_list_len = 1024;        // elements per list
  uint64_t default_list_elem_str = 256;    // bytes per string element in list
  uint64_t default_map_pairs = 512;        // number of key/value pairs
  uint64_t default_map_key_str = 128;      // bytes per string key
  uint64_t default_map_val_str = 256;      // bytes per string value
};

class ThriftSchemaParser {
 public:
  // Parse a .thrift file and extract the struct named `struct_name` into an mschema
  static std::shared_ptr<mschema> parseToMSchema(const std::string& thrift_path,
                                                 const std::string& struct_name,
                                                 const ThriftParseOptions& opt = {});

  // Convenience: return Arrow schema directly
  static std::shared_ptr<arrow::Schema> parseToArrow(const std::string& thrift_path,
                                                     const std::string& struct_name,
                                                     const ThriftParseOptions& opt = {});

  // Flatten direct struct fields into top-level Arrow fields with name prefixes.
  // Collection-wrapped structs (list<struct>, map<*,struct>) are not flattened.
  static std::shared_ptr<arrow::Schema> parseToArrowFlattened(const std::string& thrift_path,
                                                              const std::string& struct_name,
                                                              const ThriftParseOptions& opt = {},
                                                              const std::string& sep = "_",
                                                              bool flatten_list_structs = false,
                                                              bool flatten_map_structs = false);
  // Return Arrow schema plus a map: thrift field id -> top-level field index
  // Fields without an explicit numeric id are omitted from the map.
  struct ArrowSchemaWithIdMap {
    std::shared_ptr<arrow::Schema> schema;
    std::unordered_map<int, int> id_to_index;
  };
  static ArrowSchemaWithIdMap parseToArrowWithIdMap(const std::string& thrift_path,
                                                    const std::string& struct_name,
                                                    const ThriftParseOptions& opt = {});
};

} // namespace meta
} // namespace matcha
