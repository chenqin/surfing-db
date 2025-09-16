// Lightweight Arrow-only Thrift parser (no mschema/MPI dependencies)
#include "meta/thrift_arrow_lite.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <regex>
#include <sstream>

namespace matcha {
namespace meta {
namespace {

static std::string read_file(const std::string& path) {
  std::ifstream in(path);
  std::stringstream ss; ss << in.rdbuf();
  return ss.str();
}

static std::string strip_comments(const std::string& s) {
  std::string out = s;
  out = std::regex_replace(out, std::regex("/\\*.*?\\*/", std::regex::extended), "");
  out = std::regex_replace(out, std::regex("//.*?$", std::regex_constants::multiline), "");
  out = std::regex_replace(out, std::regex("#.*?$", std::regex_constants::multiline), "");
  return out;
}

static std::string trim(const std::string& s) {
  size_t b = s.find_first_not_of(" \t\r\n"); if (b==std::string::npos) return "";
  size_t e = s.find_last_not_of(" \t\r\n"); return s.substr(b, e-b+1);
}

static std::string to_lower(std::string s){ std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){return std::tolower(c);}); return s; }

enum class Prim { VOID, BOOL, I8, I32, I64, F32, UTF8 };
struct TFieldIR {
  enum Kind { PRIM, LIST, MAP, STRUCT } kind{PRIM};
  Prim prim{Prim::VOID};
  std::string struct_name;
  std::unique_ptr<TFieldIR> list_elem, map_key, map_val;
};

static Prim prim_from_token(const std::string& tok_in){
  auto tok = to_lower(tok_in);
  if (tok == "bool") return Prim::BOOL;
  if (tok == "byte" || tok == "i8") return Prim::I8;
  if (tok == "i16" || tok == "i32" || tok == "int") return Prim::I32;
  if (tok == "i64" || tok == "long") return Prim::I64;
  if (tok == "double" || tok == "float") return Prim::F32;
  if (tok == "string" || tok == "binary") return Prim::UTF8;
  return Prim::VOID;
}

static TFieldIR parse_field_type(const std::string& token) {
  std::string t = trim(token); std::string tl = to_lower(t);
  TFieldIR ir;
  if (tl.rfind("list<", 0) == 0) {
    auto gt = t.find('>'); std::string inner = t.substr(5, gt - 5);
    ir.kind = TFieldIR::LIST; ir.list_elem = std::make_unique<TFieldIR>(parse_field_type(inner)); return ir;
  }
  if (tl.rfind("map<", 0) == 0) {
    auto gt = t.find('>'); std::string inner = t.substr(4, gt - 4); auto comma = inner.find(',');
    std::string k = trim(inner.substr(0, comma)); std::string v = trim(inner.substr(comma + 1));
    ir.kind = TFieldIR::MAP; ir.map_key = std::make_unique<TFieldIR>(parse_field_type(k)); ir.map_val = std::make_unique<TFieldIR>(parse_field_type(v)); return ir;
  }
  auto p = prim_from_token(t);
  if (p != Prim::VOID) { ir.kind = TFieldIR::PRIM; ir.prim = p; }
  else { ir.kind = TFieldIR::STRUCT; ir.struct_name = t; }
  return ir;
}

struct FieldSpec { int id{0}; std::string name; TFieldIR ir; };

static std::unordered_map<std::string, std::vector<FieldSpec>> parse_all_structs_with_ids(const std::string& thrift_txt) {
  std::unordered_map<std::string, std::vector<FieldSpec>> out;
  std::string txt = strip_comments(thrift_txt);
  std::regex all_structs_re("struct\\s+([A-Za-z0-9_]+)\\s*\\{([\\s\\S]*?)\\}");
  auto begin = std::sregex_iterator(txt.begin(), txt.end(), all_structs_re);
  auto end = std::sregex_iterator();
  for (auto it = begin; it != end; ++it) {
    std::string sname = (*it)[1].str(); std::string body = (*it)[2].str();
    std::stringstream ss(body); std::string line; std::vector<FieldSpec> fields;
    while (std::getline(ss, line)) {
      std::smatch lm; std::regex line_re("\\s*([0-9]+)\\s*:\\s*(?:optional|required)?\\s*([^,;]+?)\\s+([A-Za-z0-9_]+)\\s*[,;]?\\s*$");
      if (std::regex_match(line, lm, line_re)) {
        int id = std::stoi(lm[1].str()); std::string type_tok = trim(lm[2].str()); std::string name = lm[3].str();
        fields.push_back({id, name, parse_field_type(type_tok)});
      }
    }
    out.emplace(sname, std::move(fields));
  }
  return out;
}

static std::shared_ptr<arrow::DataType> to_arrow_dt(const TFieldIR& ir,
    const std::unordered_map<std::string, std::vector<FieldSpec>>& table) {
  switch (ir.kind) {
    case TFieldIR::PRIM:
      switch (ir.prim) {
        case Prim::BOOL: return arrow::boolean();
        case Prim::I8:   return arrow::int8();
        case Prim::I32:  return arrow::int32();
        case Prim::I64:  return arrow::int64();
        case Prim::F32:  return arrow::float32();
        case Prim::UTF8: return arrow::utf8();
        default: return arrow::null();
      }
    case TFieldIR::LIST: return arrow::list(to_arrow_dt(*ir.list_elem, table));
    case TFieldIR::MAP:  return arrow::map(to_arrow_dt(*ir.map_key, table), to_arrow_dt(*ir.map_val, table));
    case TFieldIR::STRUCT: {
      std::vector<std::shared_ptr<arrow::Field>> child_fields;
      auto it2 = table.find(ir.struct_name);
      if (it2 != table.end()) {
        for (auto& p : it2->second) child_fields.push_back(arrow::field(p.name, to_arrow_dt(p.ir, table)));
      }
      return arrow::struct_(child_fields);
    }
  }
  return arrow::null();
}

} // namespace

std::shared_ptr<arrow::Schema> ThriftArrowLiteParser::parseToArrow(const std::string& thrift_path,
                                                                   const std::string& struct_name) {
  auto txt = read_file(thrift_path);
  auto table = parse_all_structs_with_ids(txt);
  auto it = table.find(struct_name);
  if (it == table.end()) return arrow::schema({});
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(it->second.size());
  for (auto& f : it->second) fields.push_back(arrow::field(f.name, to_arrow_dt(f.ir, table)));
  return arrow::schema(fields);
}

ArrowSchemaWithIdMapLite ThriftArrowLiteParser::parseToArrowWithIdMap(const std::string& thrift_path,
                                                                      const std::string& struct_name) {
  ArrowSchemaWithIdMapLite out;
  auto txt = read_file(thrift_path);
  auto table = parse_all_structs_with_ids(txt);
  auto it = table.find(struct_name);
  if (it == table.end()) { out.schema = arrow::schema({}); return out; }
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(it->second.size());
  for (size_t i = 0; i < it->second.size(); ++i) {
    const auto& f = it->second[i];
    fields.push_back(arrow::field(f.name, to_arrow_dt(f.ir, table)));
    out.id_to_index[(int)f.id] = (int)i;
  }
  out.schema = arrow::schema(fields);
  // Build nested id maps (only for list<struct> and struct fields)
  out.nested_id_maps.resize(fields.size());
  out.nested_struct_names.resize(fields.size());
  for (size_t i = 0; i < it->second.size(); ++i) {
    const auto& f = it->second[i];
    std::string nested_name;
    if (f.ir.kind == TFieldIR::STRUCT) nested_name = f.ir.struct_name;
    if (f.ir.kind == TFieldIR::LIST && f.ir.list_elem && f.ir.list_elem->kind == TFieldIR::STRUCT) nested_name = f.ir.list_elem->struct_name;
    if (!nested_name.empty()) {
      auto itn = table.find(nested_name);
      if (itn != table.end()) {
        std::unordered_map<int,int> m;
        for (size_t j=0;j<itn->second.size();++j) m[itn->second[j].id] = (int)j;
        out.nested_id_maps[i] = std::move(m);
        out.nested_struct_names[i] = nested_name;
      }
    }
  }
  return out;
}

} // namespace meta
} // namespace matcha
