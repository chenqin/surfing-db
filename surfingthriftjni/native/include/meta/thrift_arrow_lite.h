// Arrow-only Thrift IDL parser used by JNI and lightweight consumers
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <arrow/api.h>

namespace matcha {
namespace meta {

struct ArrowSchemaWithIdMapLite {
  std::shared_ptr<arrow::Schema> schema;
  std::unordered_map<int, int> id_to_index; // thrift field id -> top-level field index
  // For each top-level field: if it is list<struct> or struct, an id->index map for the nested struct
  std::vector<std::unordered_map<int, int>> nested_id_maps;
  // Name of nested struct for each top-level field if applicable (empty otherwise)
  std::vector<std::string> nested_struct_names;
};

class ThriftArrowLiteParser {
 public:
  static std::shared_ptr<arrow::Schema> parseToArrow(const std::string& thrift_path,
                                                     const std::string& struct_name);

  static ArrowSchemaWithIdMapLite parseToArrowWithIdMap(const std::string& thrift_path,
                                                        const std::string& struct_name);
};

} // namespace meta
} // namespace matcha

