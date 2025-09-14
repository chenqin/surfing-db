#include "thrift_parser.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <regex>
#include <sstream>

using matcha::meta::SchemaUtils;

namespace matcha {
namespace meta {

static std::string read_file(const std::string& path) {
  std::ifstream in(path);
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

static std::string strip_comments(const std::string& s) {
  // Remove /* ... */ and // ... endline and # ... endline
  std::string out = s;
  // Block comments
  out = std::regex_replace(out, std::regex("/\\*.*?\\*/", std::regex::extended), "");
  // Line comments starting with //
  out = std::regex_replace(out, std::regex("//.*?$", std::regex_constants::multiline), "");
  // Line comments starting with #
  out = std::regex_replace(out, std::regex("#.*?$", std::regex_constants::multiline), "");
  return out;
}

static std::string to_lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
  return s;
}

RowType::type ThriftSchemaParser::baseTypeFromToken(const std::string& tok_in) {
  auto tok = to_lower(tok_in);
  if (tok == "bool") return RowType::BOOL;
  if (tok == "byte" || tok == "i8") return RowType::CHAR;
  if (tok == "i16") return RowType::INT; // map to 32-bit slot
  if (tok == "i32" || tok == "int" ) return RowType::INT;
  if (tok == "i64" || tok == "long") return RowType::LONG;
  if (tok == "double" || tok == "float") return RowType::DOUBLE;
  if (tok == "string" || tok == "binary") return RowType::STRING;
  return RowType::VOID;
}

static std::string trim(const std::string& s) {
  size_t b = s.find_first_not_of(" \t\r\n");
  if (b == std::string::npos) return "";
  size_t e = s.find_last_not_of(" \t\r\n");
  return s.substr(b, e - b + 1);
}

// Thrift field IR for Arrow conversion (and limited mschema support)
struct TFieldIR {
  enum Kind { PRIM, LIST, MAP, STRUCT } kind{PRIM};
  RowType::type prim{RowType::VOID};
  std::string struct_name;
  std::unique_ptr<TFieldIR> list_elem;
  std::unique_ptr<TFieldIR> map_key;
  std::unique_ptr<TFieldIR> map_val;
};

static bool is_primitive_token(const std::string& tok) {
  return ThriftSchemaParser::baseTypeFromToken(tok) != RowType::VOID;
}

static TFieldIR parse_field_type(const std::string& token) {
  std::string t = trim(token);
  std::string tl = to_lower(t);
  TFieldIR ir;
  if (tl.rfind("list<", 0) == 0) {
    auto gt = t.find('>');
    std::string inner = t.substr(5, gt - 5);
    ir.kind = TFieldIR::LIST;
    ir.list_elem = std::make_unique<TFieldIR>(parse_field_type(inner));
    return ir;
  }
  if (tl.rfind("map<", 0) == 0) {
    auto gt = t.find('>');
    std::string inner = t.substr(4, gt - 4);
    auto comma = inner.find(',');
    std::string k = trim(inner.substr(0, comma));
    std::string v = trim(inner.substr(comma + 1));
    ir.kind = TFieldIR::MAP;
    ir.map_key = std::make_unique<TFieldIR>(parse_field_type(k));
    ir.map_val = std::make_unique<TFieldIR>(parse_field_type(v));
    return ir;
  }
  auto bt = ThriftSchemaParser::baseTypeFromToken(t);
  if (bt != RowType::VOID) {
    ir.kind = TFieldIR::PRIM;
    ir.prim = bt;
  } else {
    ir.kind = TFieldIR::STRUCT;
    ir.struct_name = t;
  }
  return ir;
}

// Parse all struct definitions into a symbol table: name -> vector of (field_name, IR)
static std::unordered_map<std::string, std::vector<std::pair<std::string, TFieldIR>>>
parse_all_structs(const std::string& thrift_txt) {
  std::unordered_map<std::string, std::vector<std::pair<std::string, TFieldIR>>> out;
  std::string txt = strip_comments(thrift_txt);
  std::regex all_structs_re("struct\\s+([A-Za-z0-9_]+)\\s*\\{([\\s\\S]*?)\\}");
  auto begin = std::sregex_iterator(txt.begin(), txt.end(), all_structs_re);
  auto end = std::sregex_iterator();
  for (auto it = begin; it != end; ++it) {
    std::string sname = (*it)[1].str();
    std::string body = (*it)[2].str();
    std::stringstream ss(body);
    std::string line;
    std::vector<std::pair<std::string, TFieldIR>> fields;
    while (std::getline(ss, line)) {
      line = trim(line);
      if (line.empty()) continue;
      auto colon = line.find(":");
      if (colon != std::string::npos) line = trim(line.substr(colon + 1));
      if (!line.empty() && (line.back() == ',' || line.back() == ';')) line.pop_back();
      if (to_lower(line).rfind("optional ", 0) == 0) line = trim(line.substr(9));
      if (to_lower(line).rfind("required ", 0) == 0) line = trim(line.substr(9));

      std::stringstream ls(line);
      std::string type_tok, name_tok;
      ls >> type_tok >> name_tok;
      if (type_tok.empty() || name_tok.empty()) continue;
      // type token may be list<...> or map<...>
      TFieldIR ir = parse_field_type(type_tok);
      fields.emplace_back(name_tok, std::move(ir));
    }
    out.emplace(sname, std::move(fields));
  }
  return out;
}

std::shared_ptr<mschema> ThriftSchemaParser::parseToMSchema(const std::string& thrift_path,
                                                            const std::string& struct_name,
                                                            const ThriftParseOptions& opt) {
  auto txt = read_file(thrift_path);
  auto table = parse_all_structs(txt);
  if (table.find(struct_name) == table.end()) {
    throw std::runtime_error("Struct not found in thrift: " + struct_name);
  }
  RowSchema r;

  std::function<void(const std::string&, const TFieldIR&)> emit;
  emit = [&](const std::string& name, const TFieldIR& ir){
    switch (ir.kind) {
      case TFieldIR::PRIM: {
        uint64_t max_unit = (ir.prim == RowType::STRING) ? opt.default_string_max : 0;
        SchemaUtils::initField(r, name, ir.prim, max_unit);
        break;
      }
      case TFieldIR::LIST: {
        if (ir.list_elem->kind != TFieldIR::PRIM)
          throw std::runtime_error("mschema does not support list of non-primitive types");
        uint64_t elem_sz = (ir.list_elem->prim == RowType::STRING) ? opt.default_list_elem_str : 0;
        SchemaUtils::initListField(r, name, ir.list_elem->prim, opt.default_list_len, elem_sz);
        break;
      }
      case TFieldIR::MAP: {
        if (ir.map_key->kind != TFieldIR::PRIM || ir.map_val->kind != TFieldIR::PRIM)
          throw std::runtime_error("mschema does not support map with struct/list types");
        uint64_t ksz = (ir.map_key->prim == RowType::STRING) ? opt.default_map_key_str : 0;
        uint64_t vsz = (ir.map_val->prim == RowType::STRING) ? opt.default_map_val_str : 0;
        SchemaUtils::initMapField(r, name, ir.map_key->prim, ir.map_val->prim, opt.default_map_pairs, ksz, vsz);
        break;
      }
      case TFieldIR::STRUCT: {
        // Flatten nested struct fields into mschema fields with prefix
        auto it = table.find(ir.struct_name);
        if (it == table.end()) {
          throw std::runtime_error("Unknown struct type: " + ir.struct_name);
        }
        for (auto& child : it->second) {
          std::string child_name = name + "_" + child.first;
          // Only allow primitives / list<prim> / map<prim,prim> in flattening
          if (child.second.kind == TFieldIR::STRUCT) {
            // recursively flatten deeper
            emit(child_name, child.second);
          } else if (child.second.kind == TFieldIR::PRIM || child.second.kind == TFieldIR::LIST || child.second.kind == TFieldIR::MAP) {
            emit(child_name, child.second);
          }
        }
        break;
      }
    }
  };

  for (auto& p : table[struct_name]) {
    emit(p.first, p.second);
  }
  return std::make_shared<mschema>(r);
}

std::shared_ptr<arrow::Schema> ThriftSchemaParser::parseToArrow(const std::string& thrift_path,
                                                                const std::string& struct_name,
                                                                const ThriftParseOptions& opt) {
  // Build Arrow schema from full symbol table, with nested struct support
  auto txt = read_file(thrift_path);
  auto table = parse_all_structs(txt);
  if (table.find(struct_name) == table.end()) {
    throw std::runtime_error("Struct not found in thrift: " + struct_name);
  }
  std::function<std::shared_ptr<arrow::DataType>(const TFieldIR&)> toArrow;
  toArrow = [&](const TFieldIR& ir) -> std::shared_ptr<arrow::DataType> {
    switch (ir.kind) {
      case TFieldIR::PRIM: {
        switch (ir.prim) {
          case RowType::BOOL: return arrow::boolean();
          case RowType::CHAR: return arrow::int8();
          case RowType::INT: return arrow::int32();
          case RowType::LONG: return arrow::int64();
          case RowType::DOUBLE: return arrow::float32();
          case RowType::STRING: return arrow::utf8();
          default: return arrow::null();
        }
      }
      case TFieldIR::LIST: {
        return arrow::list(toArrow(*ir.list_elem));
      }
      case TFieldIR::MAP: {
        // Enforce primitive key per Arrow's best practices
        if (ir.map_key->kind != TFieldIR::PRIM) {
          throw std::runtime_error("Arrow map key must be primitive");
        }
        return arrow::map(toArrow(*ir.map_key), toArrow(*ir.map_val));
      }
      case TFieldIR::STRUCT: {
        auto it = table.find(ir.struct_name);
        if (it == table.end()) {
          throw std::runtime_error("Unknown struct type: " + ir.struct_name);
        }
        std::vector<std::shared_ptr<arrow::Field>> child_fields;
        child_fields.reserve(it->second.size());
        for (auto& p : it->second) {
          child_fields.push_back(arrow::field(p.first, toArrow(p.second)));
        }
        return arrow::struct_(child_fields);
      }
    }
    return arrow::null();
  };

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(table[struct_name].size());
  for (auto& p : table[struct_name]) {
    fields.push_back(arrow::field(p.first, toArrow(p.second)));
  }
  return arrow::schema(fields);
}

std::shared_ptr<arrow::Schema> ThriftSchemaParser::parseToArrowFlattened(
    const std::string& thrift_path,
    const std::string& struct_name,
    const ThriftParseOptions& opt,
    const std::string& sep,
    bool flatten_list_structs,
    bool flatten_map_structs) {
  (void)opt;
  auto txt = read_file(thrift_path);
  auto table = parse_all_structs(txt);
  if (table.find(struct_name) == table.end()) {
    throw std::runtime_error("Struct not found in thrift: " + struct_name);
  }
  std::function<std::shared_ptr<arrow::DataType>(const TFieldIR&)> toArrowPrim;
  toArrowPrim = [&](const TFieldIR& ir) -> std::shared_ptr<arrow::DataType> {
    if (ir.kind == TFieldIR::PRIM) {
      switch (ir.prim) {
        case RowType::BOOL: return arrow::boolean();
        case RowType::CHAR: return arrow::int8();
        case RowType::INT: return arrow::int32();
        case RowType::LONG: return arrow::int64();
        case RowType::DOUBLE: return arrow::float32();
        case RowType::STRING: return arrow::utf8();
        default: return arrow::null();
      }
    } else if (ir.kind == TFieldIR::LIST) {
      return arrow::list(toArrowPrim(*ir.list_elem));
    } else if (ir.kind == TFieldIR::MAP) {
      if (ir.map_key->kind != TFieldIR::PRIM) throw std::runtime_error("Arrow map key must be primitive");
      return arrow::map(toArrowPrim(*ir.map_key), toArrowPrim(*ir.map_val));
    } else if (ir.kind == TFieldIR::STRUCT) {
      // Return struct type for nested collections; flatten logic will avoid recursing under collections
      auto it = table.find(ir.struct_name);
      if (it == table.end()) throw std::runtime_error("Unknown struct type: " + ir.struct_name);
      std::vector<std::shared_ptr<arrow::Field>> child_fields;
      for (auto& p : it->second) child_fields.push_back(arrow::field(p.first, toArrowPrim(p.second)));
      return arrow::struct_(child_fields);
    }
    return arrow::null();
  };

  std::vector<std::shared_ptr<arrow::Field>> out_fields;
  std::function<void(const std::string&, const TFieldIR&)> emit;
  emit = [&](const std::string& name, const TFieldIR& ir){
    if (ir.kind == TFieldIR::STRUCT) {
      auto it = table.find(ir.struct_name);
      if (it == table.end()) throw std::runtime_error("Unknown struct type: " + ir.struct_name);
      for (auto& p : it->second) {
        emit(name + sep + p.first, p.second);
      }
      return;
    }
    if (ir.kind == TFieldIR::LIST && flatten_list_structs && ir.list_elem->kind == TFieldIR::STRUCT) {
      auto it = table.find(ir.list_elem->struct_name);
      if (it == table.end()) throw std::runtime_error("Unknown struct type: " + ir.list_elem->struct_name);
      for (auto& p : it->second) {
        out_fields.push_back(arrow::field(name + sep + p.first, arrow::list(toArrowPrim(p.second))));
      }
      return;
    }
    if (ir.kind == TFieldIR::MAP && flatten_map_structs && ir.map_val->kind == TFieldIR::STRUCT) {
      if (ir.map_key->kind != TFieldIR::PRIM) throw std::runtime_error("Arrow map key must be primitive");
      auto it = table.find(ir.map_val->struct_name);
      if (it == table.end()) throw std::runtime_error("Unknown struct type: " + ir.map_val->struct_name);
      for (auto& p : it->second) {
        out_fields.push_back(arrow::field(name + sep + p.first, arrow::map(toArrowPrim(*ir.map_key), toArrowPrim(p.second))));
      }
      return;
    }
    // For collections, do not flatten further even if element is struct
    out_fields.push_back(arrow::field(name, toArrowPrim(ir)));
  };

  for (auto& p : table[struct_name]) emit(p.first, p.second);
  return arrow::schema(out_fields);
}

} // namespace meta
} // namespace matcha
