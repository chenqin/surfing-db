#include <iostream>
#include <string>
#include <arrow/api.h>
#include "meta/thrift_parser.h"

using namespace matcha::meta;

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <file.thrift> <StructName> [--flatten] [--sep=_] [--flatten-list-structs] [--flatten-map-structs] [--raw]" << std::endl;
    return 2;
  }
  std::string path = argv[1];
  std::string sname = argv[2];
  bool flatten = false;
  bool flat_list = false;
  bool flat_map = false;
  std::string sep = "_";
  bool raw = false;
  for (int i = 3; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--flatten") flatten = true;
    else if (a.rfind("--sep=", 0) == 0) sep = a.substr(6);
    else if (a == "--flatten-list-structs") flat_list = true;
    else if (a == "--flatten-map-structs") flat_map = true;
    else if (a == "--raw") raw = true;
  }
  try {
    ThriftParseOptions opt;
    auto schema = flatten ? ThriftSchemaParser::parseToArrowFlattened(path, sname, opt, sep, flat_list, flat_map)
                          : ThriftSchemaParser::parseToArrow(path, sname, opt);
    if (raw) {
      std::cout << schema->ToString() << std::endl;
    } else {
      // Pretty print: one field per line as: name: type
      std::function<std::string(std::shared_ptr<arrow::DataType>)> fmt;
      fmt = [&](std::shared_ptr<arrow::DataType> dt) -> std::string {
        switch (dt->id()) {
          case arrow::Type::NA: return "null";
          case arrow::Type::BOOL: return "bool";
          case arrow::Type::INT8: return "int8";
          case arrow::Type::INT16: return "int16";
          case arrow::Type::INT32: return "int32";
          case arrow::Type::INT64: return "int64";
          case arrow::Type::FLOAT: return "float32";
          case arrow::Type::DOUBLE: return "float64";
          case arrow::Type::STRING: return "utf8";
          case arrow::Type::BINARY: return "binary";
          case arrow::Type::LIST: {
            auto l = std::static_pointer_cast<arrow::ListType>(dt);
            return std::string("list<") + fmt(l->value_type()) + ">";
          }
          case arrow::Type::MAP: {
            auto m = std::static_pointer_cast<arrow::MapType>(dt);
            return std::string("map<") + fmt(m->key_type()) + ", " + fmt(m->item_type()) + ">";
          }
          case arrow::Type::STRUCT: {
            auto s = std::static_pointer_cast<arrow::StructType>(dt);
            std::string inner = "struct<";
            for (int i = 0; i < s->num_fields(); ++i) {
              if (i) inner += ", ";
              inner += s->field(i)->name() + ": " + fmt(s->field(i)->type());
            }
            inner += ">";
            return inner;
          }
          default:
            return dt->ToString();
        }
      };
      for (int i = 0; i < schema->num_fields(); ++i) {
        auto f = schema->field(i);
        std::cout << f->name() << ": " << fmt(f->type()) << "\n";
      }
    }
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "Error: " << ex.what() << std::endl;
    return 1;
  }
}
