/* JNI bridge for Thrift -> Arrow decoding via C++ (Binary protocol)
 * Given a list of Thrift-serialized payloads and a .thrift schema (struct name),
 * builds an Arrow RecordBatch and exports it using the Arrow C Data Interface.
 */

#include <jni.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <fstream>
#include <regex>
#include <sstream>
#include <unordered_map>

// Note: We intentionally do not depend on surfmeta (to avoid static libthrift linkage)
// and reimplement a lightweight Thrift IDL parser for schema building here.

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TType;
using apache::thrift::transport::TMemoryBuffer;

namespace {

enum class PrimTy { VOID, BOOL, I8, I32, I64, F32, UTF8 };
struct TFieldIR {
  enum Kind { PRIM, LIST, MAP, STRUCT } kind{PRIM};
  PrimTy prim{PrimTy::VOID};
  std::string struct_name;
  std::unique_ptr<TFieldIR> list_elem;
  std::unique_ptr<TFieldIR> map_key;
  std::unique_ptr<TFieldIR> map_val;
};
struct FieldSpec { int id{0}; std::string name; TFieldIR ir; };

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

// Minimal field-id + name extractor for a given struct
static std::string to_lower(std::string s){ std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){return std::tolower(c);}); return s; }
static std::string trim(const std::string& s){ size_t b=s.find_first_not_of(" \t\r\n"); if(b==std::string::npos) return ""; size_t e=s.find_last_not_of(" \t\r\n"); return s.substr(b,e-b+1);} 

static PrimTy prim_from_token(const std::string& tok_in){
  auto tok = to_lower(tok_in);
  if (tok == "bool") return PrimTy::BOOL;
  if (tok == "byte" || tok == "i8") return PrimTy::I8;
  if (tok == "i16" || tok == "i32" || tok == "int") return PrimTy::I32;
  if (tok == "i64" || tok == "long") return PrimTy::I64;
  if (tok == "double" || tok == "float") return PrimTy::F32;
  if (tok == "string" || tok == "binary") return PrimTy::UTF8;
  return PrimTy::VOID;
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
  if (p != PrimTy::VOID) { ir.kind = TFieldIR::PRIM; ir.prim = p; }
  else { ir.kind = TFieldIR::STRUCT; ir.struct_name = t; }
  return ir;
}

static std::unordered_map<std::string, std::vector<FieldSpec>> parse_all_structs_with_ids(const std::string& thrift_txt) {
  std::string txt = strip_comments(thrift_txt);
  std::unordered_map<std::string, std::vector<FieldSpec>> out;
  std::regex all_structs_re("struct\\s+([A-Za-z0-9_]+)\\s*\\{([\\s\\S]*?)\\}");
  auto begin = std::sregex_iterator(txt.begin(), txt.end(), all_structs_re);
  auto end = std::sregex_iterator();
  for (auto it = begin; it != end; ++it) {
    std::string sname = (*it)[1].str(); std::string body = (*it)[2].str();
    std::stringstream ss(body); std::string line; std::vector<FieldSpec> fields;
    while (std::getline(ss, line)) {
      std::smatch lm;
      std::regex line_re("\\s*([0-9]+)\\s*:\\s*(?:optional|required)?\\s*([^,;]+?)\\s+([A-Za-z0-9_]+)\\s*[,;]?\\s*$");
      if (std::regex_match(line, lm, line_re)) {
        int id = std::stoi(lm[1].str()); std::string type_tok = trim(lm[2].str()); std::string name = lm[3].str();
        fields.push_back({id, name, parse_field_type(type_tok)});
      }
    }
    out.emplace(sname, std::move(fields));
  }
  return out;
}

static std::unordered_map<int, int> build_id_to_index(const std::vector<FieldSpec>& specs) {
  std::unordered_map<int,int> m;
  for (size_t i = 0; i < specs.size(); ++i) m[specs[i].id] = static_cast<int>(i);
  return m;
}

static std::shared_ptr<arrow::DataType> to_arrow_dt(const TFieldIR& ir,
    const std::unordered_map<std::string, std::vector<FieldSpec>>& table);

static std::shared_ptr<arrow::DataType> to_arrow_dt(const TFieldIR& ir,
    const std::unordered_map<std::string, std::vector<FieldSpec>>& table) {
  using arrow::int8; using arrow::int32; using arrow::int64; using arrow::utf8; using arrow::float32;
  switch (ir.kind) {
    case TFieldIR::PRIM:
      switch (ir.prim) {
        case PrimTy::BOOL: return arrow::boolean();
        case PrimTy::I8:   return arrow::int8();
        case PrimTy::I32:  return arrow::int32();
        case PrimTy::I64:  return arrow::int64();
        case PrimTy::F32:  return arrow::float32();
        case PrimTy::UTF8: return arrow::utf8();
        default: return arrow::null();
      }
    case TFieldIR::LIST:
      return arrow::list(to_arrow_dt(*ir.list_elem, table));
    case TFieldIR::MAP:
      return arrow::map(to_arrow_dt(*ir.map_key, table), to_arrow_dt(*ir.map_val, table));
    case TFieldIR::STRUCT: {
      auto it = table.find(ir.struct_name); if (it == table.end()) return arrow::null();
      std::vector<std::shared_ptr<arrow::Field>> children; children.reserve(it->second.size());
      for (auto& f : it->second) children.push_back(arrow::field(f.name, to_arrow_dt(f.ir, table)));
      return arrow::struct_(children);
    }
  }
  return arrow::null();
}

// Append a null for the given builder (primitive/string types handled; complex left null)
static void AppendNull(arrow::ArrayBuilder* bldr) {
  (void)bldr->AppendNull();
}

// Decode a single primitive field from protocol and append to builder at current row
static void DecodeAndAppend(TBinaryProtocol* prot, TType ttype, arrow::ArrayBuilder* bldr) {
  switch (ttype) {
    case TType::T_BOOL: {
      bool v; prot->readBool(v); auto* vb = static_cast<arrow::BooleanBuilder*>(bldr); vb->Append(v);
      break;
    }
    case TType::T_BYTE: {
      int8_t v; prot->readByte(v); auto* vb = static_cast<arrow::Int8Builder*>(bldr); vb->Append(v);
      break;
    }
    case TType::T_I16: {
      int16_t v; prot->readI16(v); auto* vb = static_cast<arrow::Int32Builder*>(bldr); vb->Append(static_cast<int32_t>(v));
      break;
    }
    case TType::T_I32: {
      int32_t v; prot->readI32(v); auto* vb = static_cast<arrow::Int32Builder*>(bldr); vb->Append(v);
      break;
    }
    case TType::T_I64: {
      int64_t v; prot->readI64(v); auto* vb = static_cast<arrow::Int64Builder*>(bldr); vb->Append(v);
      break;
    }
    case TType::T_DOUBLE: {
      double v; prot->readDouble(v); auto* vb = static_cast<arrow::FloatBuilder*>(bldr); vb->Append(static_cast<float>(v));
      break;
    }
    case TType::T_STRING: {
      std::string s; prot->readBinary(s); auto* vb = static_cast<arrow::StringBuilder*>(bldr); vb->Append(s);
      break;
    }
    default: {
      prot->skip(ttype); bldr->AppendNull();
    }
  }
}

} // namespace

extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeThriftDecoder_decode(
    JNIEnv* env, jclass,
    jobjectArray jpayloads, jstring jpath, jstring jstruct,
    jlong schema_out_addr, jlong array_out_addr) {
  // Extract inputs
  const char* cpath = env->GetStringUTFChars(jpath, nullptr);
  const char* cstruct = env->GetStringUTFChars(jstruct, nullptr);
  std::string thrift_path(cpath ? cpath : "");
  std::string struct_name(cstruct ? cstruct : "");
  if (cpath) env->ReleaseStringUTFChars(jpath, cpath);
  if (cstruct) env->ReleaseStringUTFChars(jstruct, cstruct);

  // Parse .thrift and build table (struct symbol table with IDs and IR)
  auto txt = read_file(thrift_path);
  auto table = parse_all_structs_with_ids(txt);
  auto it_struct = table.find(struct_name);
  if (it_struct == table.end()) {
    return;
  }
  const auto& fvec = it_struct->second;
  auto id_to_index = build_id_to_index(fvec);

  // Build Arrow schema from IR
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(fvec.size());
  for (auto& f : fvec) fields.push_back(arrow::field(f.name, to_arrow_dt(f.ir, table)));
  auto schema = arrow::schema(fields);

  // Compute number of rows (payloads)
  jsize n = env->GetArrayLength(jpayloads);

  // Prepare builders per field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  builders.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    std::unique_ptr<arrow::ArrayBuilder> b;
    arrow::MakeBuilder(arrow::default_memory_pool(), schema->field(i)->type(), &b);
    // Reserve space for all rows to avoid repeated reallocations
    if (b) {
      b->Reserve(static_cast<int64_t>(n));
    }
    builders.push_back(std::move(b));
  }

  // Iterate payloads
  for (jsize r = 0; r < n; ++r) {
    jbyteArray arr = (jbyteArray) env->GetObjectArrayElement(jpayloads, r);
    if (!arr) continue;
    jsize len = env->GetArrayLength(arr);
    jboolean is_copy = JNI_FALSE;
    jbyte* bytes = env->GetByteArrayElements(arr, &is_copy);
    // Setup Thrift protocol on buffer
    auto trans = std::make_shared<TMemoryBuffer>(reinterpret_cast<uint8_t*>(bytes), static_cast<uint32_t>(len));
    TBinaryProtocol prot(trans);

    std::vector<bool> present(schema->num_fields(), false);
    std::string sname;
    prot.readStructBegin(sname);
    while (true) {
      std::string fname; TType ftype; int16_t fid;
      prot.readFieldBegin(fname, ftype, fid);
      if (ftype == TType::T_STOP) break;
      auto it = id_to_index.find(fid);
      if (it == id_to_index.end()) {
        prot.skip(ftype);
        prot.readFieldEnd();
        continue;
      }
      int idx = it->second;
      auto& b = builders[idx];
      // Decode primitives and string; skip others with null
      switch (schema->field(idx)->type()->id()) {
        case arrow::Type::NA:
          prot.skip(ftype); b->AppendNull(); present[idx] = true; break;
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE: // not used; mapping uses float32
        case arrow::Type::STRING: {
          DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true; break;
        }
        default:
          // Complex types currently unsupported in native decode; append null placeholder
          prot.skip(ftype); b->AppendNull(); present[idx] = true; break;
      }
      prot.readFieldEnd();
    }
    prot.readStructEnd();

    // Append nulls for fields not present
    for (int i = 0; i < schema->num_fields(); ++i) {
      if (!present[i]) builders[i]->AppendNull();
    }

    env->ReleaseByteArrayElements(arr, bytes, JNI_ABORT);
    env->DeleteLocalRef(arr);
  }

  // Finish arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(schema->num_fields());
  for (auto& b : builders) {
    std::shared_ptr<arrow::Array> a; b->Finish(&a); arrays.push_back(std::move(a));
  }
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(n), arrays);

  // Export via C Data
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*schema.get(), schema_out);
  arrow::ExportRecordBatch(*batch.get(), array_out, schema_out);
}
