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
#include <thread>

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
  // Keep IR for nested type resolution
  // Helper lambdas to fetch nested struct names from IR
  auto get_list_struct_name = [&](int idx)->std::string {
    if (idx < (int)fvec.size()) {
      const auto& ir = fvec[idx].ir;
      if (ir.kind == TFieldIR::LIST && ir.list_elem && ir.list_elem->kind == TFieldIR::STRUCT) return ir.list_elem->struct_name;
    }
    return std::string();
  };
  auto get_map_val_struct_name = [&](int idx)->std::string {
    if (idx < (int)fvec.size()) {
      const auto& ir = fvec[idx].ir;
      if (ir.kind == TFieldIR::MAP && ir.map_val && ir.map_val->kind == TFieldIR::STRUCT) return ir.map_val->struct_name;
    }
    return std::string();
  };

  // Build Arrow schema from IR
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(fvec.size());
  for (auto& f : fvec) fields.push_back(arrow::field(f.name, to_arrow_dt(f.ir, table)));
  auto schema = arrow::schema(fields);
  // Helpers for nested struct name resolution
  auto get_list_struct_name = [&](int idx)->std::string {
    if (idx < (int)fvec.size()) { const auto& ir = fvec[idx].ir; if (ir.kind==TFieldIR::LIST && ir.list_elem && ir.list_elem->kind==TFieldIR::STRUCT) return ir.list_elem->struct_name; }
    return std::string(); };
  auto get_map_val_struct_name = [&](int idx)->std::string {
    if (idx < (int)fvec.size()) { const auto& ir = fvec[idx].ir; if (ir.kind==TFieldIR::MAP && ir.map_val && ir.map_val->kind==TFieldIR::STRUCT) return ir.map_val->struct_name; }
    return std::string(); };

  // Compute number of rows (payloads)
  jsize n = env->GetArrayLength(jpayloads);

  // Identify numeric columns to enable columnar AppendValues fast path
  enum ColKind {
    CK_NONE,
    // scalar columns
    CK_I8, CK_I32, CK_I64, CK_F32, CK_BOOL, CK_STR,
    // list<primitive> columns
    CK_LIST_I8, CK_LIST_I32, CK_LIST_I64, CK_LIST_F32, CK_LIST_BOOL, CK_LIST_STR,
    // map<string, primitive>
    CK_MAP_STR_I64, CK_MAP_STR_F32, CK_MAP_STR_STR
  };
  struct ColBuffers {
    ColKind kind{CK_NONE};
    std::vector<int8_t>  i8;
    std::vector<int32_t> i32;
    std::vector<int64_t> i64;
    std::vector<float>   f32;
    std::vector<uint8_t> b8;   // boolean values as 0/1
    std::vector<uint8_t> valid; // 0/1 validity per row
    // strings
    std::vector<uint8_t> sdata;   // concatenated utf8 bytes
    std::vector<int32_t> soff;    // offsets length n+1
    // list<primitive>
    std::vector<int32_t> l_off;   // list offsets length rows+1
    std::vector<int8_t>  l_i8;
    std::vector<int32_t> l_i32;
    std::vector<int64_t> l_i64;
    std::vector<float>   l_f32;
    std::vector<uint8_t> l_b8;
    // list<string>
    std::vector<uint8_t> l_sdata;
    std::vector<int32_t> l_soff;
    // map<string, primitive>: entry offsets, keys string buffers, values
    std::vector<int32_t> m_off;   // map entry offsets length rows+1
    std::vector<uint8_t> mk_sdata; std::vector<int32_t> mk_soff;
    std::vector<int64_t> mv_i64;  std::vector<float> mv_f32;
    std::vector<uint8_t> mv_sdata; std::vector<int32_t> mv_soff; // string values
  };
  std::vector<ColBuffers> colbufs(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto tid = schema->field(i)->type()->id();
    ColKind k = CK_NONE;
    switch (tid) {
      case arrow::Type::INT8:   k = CK_I8; break;
      case arrow::Type::INT16:  k = CK_I32; break; // int16 maps to int32 storage
      case arrow::Type::INT32:  k = CK_I32; break;
      case arrow::Type::INT64:  k = CK_I64; break;
      case arrow::Type::FLOAT:  k = CK_F32; break;
      case arrow::Type::BOOL:   k = CK_BOOL; break;
      case arrow::Type::STRING: k = CK_STR; break;
      case arrow::Type::LIST: {
        auto lt = std::static_pointer_cast<arrow::ListType>(schema->field(i)->type());
        switch (lt->value_type()->id()) {
          case arrow::Type::INT8:  k = CK_LIST_I8; break;
          case arrow::Type::INT16: // widen to int32
          case arrow::Type::INT32: k = CK_LIST_I32; break;
          case arrow::Type::INT64: k = CK_LIST_I64; break;
          case arrow::Type::FLOAT: k = CK_LIST_F32; break;
          case arrow::Type::BOOL:  k = CK_LIST_BOOL; break;
          case arrow::Type::STRING: k = CK_LIST_STR; break;
          default: k = CK_NONE; break; // unsupported (e.g., list<struct>, list<string> for now)
        }
        break;
      }
      case arrow::Type::MAP: {
        auto mt = std::static_pointer_cast<arrow::MapType>(schema->field(i)->type());
        if (mt->key_type()->id() == arrow::Type::STRING) {
          switch (mt->item_type()->id()) {
            case arrow::Type::INT64: k = CK_MAP_STR_I64; break;
            case arrow::Type::FLOAT: k = CK_MAP_STR_F32; break;
            case arrow::Type::STRING: k = CK_MAP_STR_STR; break;
            default: k = CK_NONE; break;
          }
        }
        break;
      }
      default:                  k = CK_NONE; break;
    }
    colbufs[i].kind = k;
    if (k != CK_NONE) {
      colbufs[i].valid.reserve(n);
      switch (k) {
        case CK_I8:  colbufs[i].i8.reserve(n);  break;
        case CK_I32: colbufs[i].i32.reserve(n); break;
        case CK_I64: colbufs[i].i64.reserve(n); break;
        case CK_F32: colbufs[i].f32.reserve(n); break;
        case CK_BOOL: colbufs[i].b8.reserve(n); break;
        case CK_STR:
          colbufs[i].sdata.reserve(n * 8); // rough guess, grow as needed
          colbufs[i].soff.reserve(n + 1);
          colbufs[i].soff.push_back(0);
          break;
        case CK_LIST_I8:
        case CK_LIST_I32:
        case CK_LIST_I64:
        case CK_LIST_F32:
        case CK_LIST_BOOL:
        case CK_LIST_STR:
          colbufs[i].l_off.reserve(n + 1); colbufs[i].l_off.push_back(0);
          // reserve some space for values; exact growth happens as needed
          break;
        case CK_MAP_STR_I64:
        case CK_MAP_STR_F32:
        case CK_MAP_STR_STR:
          colbufs[i].m_off.reserve(n + 1); colbufs[i].m_off.push_back(0);
          colbufs[i].mk_soff.reserve(n + 1); colbufs[i].mk_soff.push_back(0);
          if (k == CK_MAP_STR_STR) { colbufs[i].mv_soff.reserve(n + 1); colbufs[i].mv_soff.push_back(0); }
          break;
        default: break;
      }
    }
  }

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

  // Iterate payloads in micro-batches to improve cache locality and cap memory
  const int kBatch = 4000;
  // Reuse transport/protocol objects across rows to reduce allocations
  auto mem = std::make_shared<TMemoryBuffer>();
  TBinaryProtocol prot(mem);
  // Reuse per-row presence flags (uint8_t avoids vector<bool> specialization cost)
  std::vector<uint8_t> present(static_cast<size_t>(schema->num_fields()), 0);

  for (jsize start = 0; start < n; start += kBatch) {
    jsize end = std::min<jsize>(n, start + kBatch);
    // reset per-batch columnar buffers
    for (int i = 0; i < schema->num_fields(); ++i) {
      if (colbufs[i].kind != CK_NONE) {
        colbufs[i].valid.clear();
        switch (colbufs[i].kind) {
          case CK_I8:  colbufs[i].i8.clear(); break;
          case CK_I32: colbufs[i].i32.clear(); break;
          case CK_I64: colbufs[i].i64.clear(); break;
          case CK_F32: colbufs[i].f32.clear(); break;
          case CK_BOOL: colbufs[i].b8.clear(); break;
          case CK_STR:
            colbufs[i].sdata.clear();
            colbufs[i].soff.clear();
            colbufs[i].soff.push_back(0);
            break;
          case CK_LIST_STR:
            colbufs[i].l_off.clear(); colbufs[i].l_off.push_back(0);
            colbufs[i].l_sdata.clear();
            colbufs[i].l_soff.clear(); colbufs[i].l_soff.push_back(0);
            break;
          default: break;
        }
      }
    }

    for (jsize r = start; r < end; ++r) {
      jbyteArray arr = (jbyteArray) env->GetObjectArrayElement(jpayloads, r);
      if (!arr) continue;
      jsize len = env->GetArrayLength(arr);
      // Pin Java byte[] with Critical to avoid extra copies and reduce JNI overhead
      jboolean is_copy = JNI_FALSE;
      jbyte* bytes = (jbyte*) env->GetPrimitiveArrayCritical(arr, &is_copy);
      // Reset transport to point at this payload; reuse protocol instance
      mem->resetBuffer(reinterpret_cast<uint8_t*>(bytes), static_cast<uint32_t>(len));

      // reset presence flags for this row
      std::memset(present.data(), 0, present.size());
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
        auto colk = colbufs[idx].kind;
        switch (schema->field(idx)->type()->id()) {
        case arrow::Type::NA: {
          prot.skip(ftype); b->AppendNull(); present[idx] = true; break;
        }
        case arrow::Type::BOOL: {
          if (colk == CK_BOOL && ftype == TType::T_BOOL) {
            bool v; prot.readBool(v);
            colbufs[idx].b8.push_back(v ? 1 : 0);
            colbufs[idx].valid.push_back(1);
            present[idx] = true;
          } else {
            DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true;
          }
          break;
        }
        case arrow::Type::INT8: {
          if (colk == CK_I8 && ftype == TType::T_BYTE) {
            int8_t v; prot.readByte(v);
            colbufs[idx].i8.push_back(v);
            colbufs[idx].valid.push_back(1);
            present[idx] = true;
          } else {
            DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true;
          }
          break;
        }
        case arrow::Type::INT16: // stored as int32
        case arrow::Type::INT32: {
          if (colk == CK_I32 && (ftype == TType::T_I16 || ftype == TType::T_I32)) {
            int32_t outv = 0;
            if (ftype == TType::T_I16) { int16_t t; prot.readI16(t); outv = static_cast<int32_t>(t); }
            else { int32_t t; prot.readI32(t); outv = t; }
            colbufs[idx].i32.push_back(outv);
            colbufs[idx].valid.push_back(1);
            present[idx] = true;
          } else {
            DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true;
          }
          break;
        }
        case arrow::Type::INT64: {
          if (colk == CK_I64 && ftype == TType::T_I64) {
            int64_t v; prot.readI64(v);
            colbufs[idx].i64.push_back(v);
            colbufs[idx].valid.push_back(1);
            present[idx] = true;
          } else {
            DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true;
          }
          break;
        }
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE: { // float32 builder; thrift uses T_DOUBLE
          if (colk == CK_F32 && ftype == TType::T_DOUBLE) {
            double dv; prot.readDouble(dv);
            colbufs[idx].f32.push_back(static_cast<float>(dv));
            colbufs[idx].valid.push_back(1);
            present[idx] = true;
          } else {
            DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true;
          }
          break;
        }
        case arrow::Type::STRING: {
          if (colk == CK_STR && (ftype == TType::T_STRING)) {
            // Fast path: borrow from transport directly into sdata/soff
            int32_t len = 0; prot.readI32(len);
            auto& cb = colbufs[idx];
            if (len > 0) {
              uint32_t want = static_cast<uint32_t>(len);
              const uint8_t* p = mem->borrow(nullptr, &want);
              if (p && want >= static_cast<uint32_t>(len)) {
                cb.sdata.insert(cb.sdata.end(), p, p + len);
                cb.soff.push_back(cb.soff.back() + len);
                mem->consume(static_cast<uint32_t>(len));
              } else {
                std::vector<uint8_t> tmp(len); uint32_t got = 0;
                while (got < static_cast<uint32_t>(len)) { uint32_t r = mem->read(tmp.data() + got, static_cast<uint32_t>(len) - got); if (r == 0) break; got += r; }
                cb.sdata.insert(cb.sdata.end(), tmp.begin(), tmp.end());
                cb.soff.push_back(cb.soff.back() + len);
              }
            } else {
              cb.soff.push_back(cb.soff.back());
            }
            cb.valid.push_back(1);
            present[idx] = true;
          } else {
            DecodeAndAppend(&prot, ftype, b.get()); present[idx] = true;
          }
          break;
        }
        case arrow::Type::LIST: {
          // list<primitive/string> columnar; list<struct> row-wise
          // Detect list<struct>
          auto lt = std::static_pointer_cast<arrow::ListType>(schema->field(idx)->type());
          if (lt->value_type()->id() == arrow::Type::STRUCT) {
            if (ftype == TType::T_LIST) {
              apache::thrift::protocol::TType et; uint32_t sz; prot.readListBegin(et, sz);
              if (et == TType::T_STRUCT) {
                auto* lb = static_cast<arrow::ListBuilder*>(b.get());
                auto* sb = static_cast<arrow::StructBuilder*>(lb->value_builder());
                // Find nested struct name via IR
                std::string nested_name = get_list_struct_name(idx);
                lb->Append(true);
                // Build id->pos for nested struct
                auto itn = table.find(nested_name);
                std::unordered_map<int,int> nid2pos;
                if (itn != table.end()) nid2pos = build_id_to_index(itn->second);
                for (uint32_t j = 0; j < sz; ++j) {
                  // Decode one struct element
                  std::string sname2; prot.readStructBegin(sname2);
                  sb->Append(true);
                  std::vector<uint8_t> child_present(sb->num_fields(), 0);
                  while (true) {
                    std::string fname2; TType ft2; int16_t fid2; prot.readFieldBegin(fname2, ft2, fid2);
                    if (ft2 == TType::T_STOP) break;
                    int cpos = -1; if (!nid2pos.empty()) { auto itp = nid2pos.find(fid2); if (itp != nid2pos.end()) cpos = itp->second; }
                    if (cpos >= 0) {
                      auto* cbb = sb->field_builder(cpos);
                      // Determine child IR/type
                      TFieldIR cir; if (itn != table.end() && (size_t)cpos < itn->second.size()) cir = itn->second[cpos].ir;
                      switch (sb->type()->field(cpos)->type()->id()) {
                        case arrow::Type::INT8: case arrow::Type::INT16: case arrow::Type::INT32: case arrow::Type::INT64: case arrow::Type::FLOAT: case arrow::Type::BOOL: {
                          DecodeAndAppend(&prot, ft2, cbb);
                          child_present[cpos] = 1; break;
                        }
                        case arrow::Type::STRING: {
                          if (ft2 == TType::T_STRING) {
                            // borrow append
                            int32_t len=0; prot.readI32(len);
                            auto* sbldr = static_cast<arrow::StringBuilder*>(cbb);
                            if (len > 0) {
                              uint32_t want=(uint32_t)len; const uint8_t* p = mem->borrow(nullptr,&want);
                              if (p && want >= (uint32_t)len) { sbldr->Append(reinterpret_cast<const char*>(p), len); mem->consume((uint32_t)len);} else {
                                std::string tmp; tmp.resize(len); uint32_t got=0; while (got < (uint32_t)len){ uint8_t ch; prot.readByte((int8_t&)ch); tmp[got++]= (char)ch; } sbldr->Append(tmp);
                              }
                            } else { sbldr->Append(""); }
                            child_present[cpos] = 1; break;
                          } else { prot.skip(ft2); }
                          break;
                        }
                        case arrow::Type::LIST: {
                          // Support list<string> inside struct
                          auto clt = std::static_pointer_cast<arrow::ListType>(sb->type()->field(cpos)->type());
                          if (clt->value_type()->id() == arrow::Type::STRING && ft2 == TType::T_LIST) {
                            auto* lb2 = static_cast<arrow::ListBuilder*>(cbb);
                            auto* vb2 = static_cast<arrow::StringBuilder*>(lb2->value_builder());
                            apache::thrift::protocol::TType et2; uint32_t sz2; prot.readListBegin(et2, sz2);
                            if (et2 == TType::T_STRING) {
                              lb2->Append(true);
                              for (uint32_t k=0;k<sz2;++k){ std::string s; prot.readBinary(s); vb2->Append(s); }
                            } else { prot.skip(et2); lb2->AppendNull(); }
                            prot.readListEnd(); child_present[cpos]=1; break;
                          } else { prot.skip(ft2); }
                          break;
                        }
                        case arrow::Type::MAP: {
                          // Support map<string, list<string>> inside struct
                          auto cmt = std::static_pointer_cast<arrow::MapType>(sb->type()->field(cpos)->type());
                          if (cmt->key_type()->id()==arrow::Type::STRING && cmt->item_type()->id()==arrow::Type::LIST && ft2==TType::T_MAP) {
                            auto* mb2 = static_cast<arrow::MapBuilder*>(cbb);
                            auto* kb2 = static_cast<arrow::StringBuilder*>(mb2->key_builder());
                            auto* lbv = static_cast<arrow::ListBuilder*>(mb2->item_builder());
                            auto* svb = static_cast<arrow::StringBuilder*>(lbv->value_builder());
                            apache::thrift::protocol::TType kt, vt; uint32_t msz; prot.readMapBegin(kt, vt, msz);
                            if (kt==TType::T_STRING && vt==TType::T_LIST) {
                              for (uint32_t e=0;e<msz;++e){ std::string k; prot.readBinary(k); kb2->Append(k); apache::thrift::protocol::TType et3; uint32_t ls; prot.readListBegin(et3, ls); if (et3==TType::T_STRING){ lbv->Append(true); for (uint32_t t=0;t<ls;++t){ std::string s; prot.readBinary(s); svb->Append(s);} } else { lbv->AppendNull(); prot.skip(et3);} prot.readListEnd(); }
                              mb2->Append();
                            } else { prot.skip(vt); mb2->AppendNull(); }
                            prot.readMapEnd(); child_present[cpos]=1; break;
                          } else { prot.skip(ft2); }
                          break;
                        }
                        default: prot.skip(ft2); break;
                      }
                    } else { prot.skip(ft2); }
                    prot.readFieldEnd();
                  }
                  prot.readStructEnd();
                  // pad missing children
                  for (int cc=0; cc<sb->num_fields(); ++cc) if (!child_present[cc]) sb->field_builder(cc)->AppendNull();
                }
                prot.readListEnd(); present[idx]=true; break;
              } else {
                prot.skip(et); present[idx]=true; break;
              }
            } else { present[idx]=true; }
            break;
          }
          // list<primitive> columnar
          switch (colk) {
            case CK_LIST_I8:
            case CK_LIST_I32:
            case CK_LIST_I64:
            case CK_LIST_F32:
            case CK_LIST_BOOL: {
              if (ftype == TType::T_LIST) {
                apache::thrift::protocol::TType et; uint32_t sz;
                prot.readListBegin(et, sz);
                // accumulate values
                auto& cb = colbufs[idx];
                switch (colk) {
                  case CK_LIST_I8: {
                    if (et == TType::T_BYTE) {
                      cb.l_i8.reserve(cb.l_i8.size() + sz);
                      for (uint32_t j = 0; j < sz; ++j) { int8_t v; prot.readByte(v); cb.l_i8.push_back(v); }
                      cb.valid.push_back(1);
                      cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                    } else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                    break;
                  }
                  case CK_LIST_I32: {
                    if (et == TType::T_I16 || et == TType::T_I32) {
                      cb.l_i32.reserve(cb.l_i32.size() + sz);
                      for (uint32_t j = 0; j < sz; ++j) { if (et==TType::T_I16){ int16_t v; prot.readI16(v); cb.l_i32.push_back(static_cast<int32_t>(v)); } else { int32_t v; prot.readI32(v); cb.l_i32.push_back(v);} }
                      cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                    } else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                    break;
                  }
                  case CK_LIST_I64: {
                    if (et == TType::T_I64) {
                      cb.l_i64.reserve(cb.l_i64.size() + sz);
                      for (uint32_t j = 0; j < sz; ++j) { int64_t v; prot.readI64(v); cb.l_i64.push_back(v); }
                      cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                    } else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                    break;
                  }
                  case CK_LIST_F32: {
                    if (et == TType::T_DOUBLE) {
                      cb.l_f32.reserve(cb.l_f32.size() + sz);
                      for (uint32_t j = 0; j < sz; ++j) { double dv; prot.readDouble(dv); cb.l_f32.push_back(static_cast<float>(dv)); }
                      cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                    } else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                    break;
                  }
                  case CK_LIST_BOOL: {
                    if (et == TType::T_BOOL) {
                      cb.l_b8.reserve(cb.l_b8.size() + sz);
                      for (uint32_t j = 0; j < sz; ++j) { bool v; prot.readBool(v); cb.l_b8.push_back(v ? 1 : 0); }
                      cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                    } else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                    break;
                  }
                  case CK_LIST_STR: {
                    if (et == TType::T_STRING) {
                      for (uint32_t j = 0; j < sz; ++j) {
                        int32_t slen = 0; prot.readI32(slen);
                        if (slen > 0) {
                          uint32_t want = static_cast<uint32_t>(slen);
                          const uint8_t* p = mem->borrow(nullptr, &want);
                          if (p && want >= static_cast<uint32_t>(slen)) {
                            cb.l_sdata.insert(cb.l_sdata.end(), p, p + slen);
                            cb.l_soff.push_back(cb.l_soff.back() + slen);
                            mem->consume(static_cast<uint32_t>(slen));
                          } else {
                            std::vector<uint8_t> tmp(slen); uint32_t got = 0;
                            while (got < static_cast<uint32_t>(slen)) { uint32_t r = mem->read(tmp.data() + got, static_cast<uint32_t>(slen) - got); if (r == 0) break; got += r; }
                            cb.l_sdata.insert(cb.l_sdata.end(), tmp.begin(), tmp.end());
                            cb.l_soff.push_back(cb.l_soff.back() + slen);
                          }
                        } else {
                          cb.l_soff.push_back(cb.l_soff.back());
                        }
                      }
                      cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                    } else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                    break;
                  }
                  default: break;
                }
                prot.readListEnd(); present[idx] = true;
              } else {
                present[idx] = true; // treat as present but invalid type; we'll add placeholder below
              }
              break;
            }
            default: {
              // unsupported list schema element
              prot.skip(ftype); b->AppendNull(); present[idx] = true; break;
            }
          }
          break;
        }
        case arrow::Type::MAP: {
          // map<string, struct> row-wise
          auto mt = std::static_pointer_cast<arrow::MapType>(schema->field(idx)->type());
          if (mt->key_type()->id()==arrow::Type::STRING && mt->item_type()->id()==arrow::Type::STRUCT) {
            if (ftype == TType::T_MAP) {
              apache::thrift::protocol::TType kt, vt; uint32_t sz; prot.readMapBegin(kt, vt, sz);
              auto* mb = static_cast<arrow::MapBuilder*>(b.get());
              auto* kb = static_cast<arrow::StringBuilder*>(mb->key_builder());
              auto* sb = static_cast<arrow::StructBuilder*>(mb->item_builder());
              std::string nested_name = get_map_val_struct_name(idx);
              auto itn = table.find(nested_name); std::unordered_map<int,int> nid2pos; if (itn!=table.end()) nid2pos = build_id_to_index(itn->second);
              if (kt==TType::T_STRING && vt==TType::T_STRUCT) {
                mb->Append();
                for (uint32_t e=0;e<sz;++e){ std::string k; prot.readBinary(k); kb->Append(k); std::string sn; prot.readStructBegin(sn); sb->Append(true); std::vector<uint8_t> child_present(sb->num_fields(),0); while (true){ std::string fn; TType ft2; int16_t fid; prot.readFieldBegin(fn, ft2, fid); if (ft2==TType::T_STOP) break; int cpos=-1; if (!nid2pos.empty()){ auto itp=nid2pos.find(fid); if (itp!=nid2pos.end()) cpos=itp->second; } if (cpos>=0){ auto* cbb=sb->field_builder(cpos); switch (sb->type()->field(cpos)->type()->id()) { case arrow::Type::INT64: case arrow::Type::INT32: case arrow::Type::INT8: case arrow::Type::FLOAT: case arrow::Type::BOOL: { DecodeAndAppend(&prot, ft2, cbb); child_present[cpos]=1; break;} case arrow::Type::STRING: { if (ft2==TType::T_STRING){ std::string s; prot.readBinary(s); static_cast<arrow::StringBuilder*>(cbb)->Append(s); child_present[cpos]=1;} else { prot.skip(ft2);} break;} case arrow::Type::LIST: { // support list<string>
                      auto clt = std::static_pointer_cast<arrow::ListType>(sb->type()->field(cpos)->type()); if (clt->value_type()->id()==arrow::Type::STRING && ft2==TType::T_LIST){ auto* lb2=static_cast<arrow::ListBuilder*>(cbb); auto* svb=static_cast<arrow::StringBuilder*>(lb2->value_builder()); apache::thrift::protocol::TType et2; uint32_t lsz; prot.readListBegin(et2, lsz); if (et2==TType::T_STRING){ lb2->Append(true); for (uint32_t t=0;t<lsz;++t){ std::string s; prot.readBinary(s); svb->Append(s);} } else { lb2->AppendNull(); prot.skip(et2);} prot.readListEnd(); child_present[cpos]=1; } else { prot.skip(ft2);} break; } default: prot.skip(ft2); break;} } else { prot.skip(ft2);} prot.readFieldEnd(); } prot.readStructEnd(); for (int cc=0; cc<sb->num_fields(); ++cc) if (!child_present[cc]) sb->field_builder(cc)->AppendNull(); }
              } else { prot.skip(vt); }
              prot.readMapEnd(); present[idx]=true;
            } else { present[idx]=true; }
            break;
          }
          switch (colk) {
            case CK_MAP_STR_I64:
            case CK_MAP_STR_F32:
            case CK_MAP_STR_STR: {
              if (ftype == TType::T_MAP) {
                apache::thrift::protocol::TType kt, vt; uint32_t sz;
                prot.readMapBegin(kt, vt, sz);
                auto& cb = colbufs[idx];
                if (kt == TType::T_STRING) {
                  for (uint32_t j = 0; j < sz; ++j) {
                    std::string k; prot.readBinary(k);
                    cb.mk_sdata.reserve(cb.mk_sdata.size() + k.size());
                    cb.mk_sdata.insert(cb.mk_sdata.end(), reinterpret_cast<const uint8_t*>(k.data()), reinterpret_cast<const uint8_t*>(k.data()) + k.size());
                    cb.mk_soff.push_back(cb.mk_soff.back() + static_cast<int32_t>(k.size()));
                    switch (colk) {
                      case CK_MAP_STR_I64: { int64_t v; prot.readI64(v); cb.mv_i64.push_back(v); break; }
                      case CK_MAP_STR_F32: { double dv; prot.readDouble(dv); cb.mv_f32.push_back(static_cast<float>(dv)); break; }
                      case CK_MAP_STR_STR: { std::string vs; prot.readBinary(vs); cb.mv_sdata.reserve(cb.mv_sdata.size() + vs.size()); cb.mv_sdata.insert(cb.mv_sdata.end(), reinterpret_cast<const uint8_t*>(vs.data()), reinterpret_cast<const uint8_t*>(vs.data()) + vs.size()); cb.mv_soff.push_back(cb.mv_soff.back() + static_cast<int32_t>(vs.size())); break; }
                      default: break;
                    }
                  }
                  cb.valid.push_back(1); cb.m_off.push_back(cb.m_off.back() + static_cast<int32_t>(sz));
                } else {
                  // skip unsupported key type
                  prot.skip(vt); cb.valid.push_back(0); cb.m_off.push_back(cb.m_off.back());
                }
                prot.readMapEnd(); present[idx] = true;
              } else {
                present[idx] = true; // treat as present but invalid type (placeholder below)
              }
              break;
            }
            default: {
              prot.skip(ftype); b->AppendNull(); present[idx] = true; break;
            }
          }
          break;
        }
        default: {
          // Complex types currently unsupported in native decode; append null placeholder
          prot.skip(ftype); b->AppendNull(); present[idx] = true; break;
        }
      }
        prot.readFieldEnd();
      }
      prot.readStructEnd();

      // Append placeholders for fields not present
      for (int i = 0; i < schema->num_fields(); ++i) {
        if (!present[i]) {
          if (colbufs[i].kind != CK_NONE) {
            // Columnar path: add default 0 value and mark invalid
            colbufs[i].valid.push_back(0);
            switch (colbufs[i].kind) {
              case CK_I8:   colbufs[i].i8.push_back(0); break;
              case CK_I32:  colbufs[i].i32.push_back(0); break;
              case CK_I64:  colbufs[i].i64.push_back(0); break;
              case CK_F32:  colbufs[i].f32.push_back(0.0f); break;
              case CK_BOOL: colbufs[i].b8.push_back(0); break;
              case CK_STR: {
                // no data bytes; repeat last offset
                auto& cb = colbufs[i];
                cb.soff.push_back(cb.soff.back());
                break;
              }
              default: break;
            }
          } else {
            builders[i]->AppendNull();
          }
        }
      }

      env->ReleasePrimitiveArrayCritical(arr, bytes, JNI_ABORT);
      env->DeleteLocalRef(arr);
    }

    // Flush this micro-batch into builders
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto& b = builders[i];
      switch (colbufs[i].kind) {
        case CK_I8: {
          auto* nb = static_cast<arrow::Int8Builder*>(b.get());
          nb->AppendValues(colbufs[i].i8.data(), static_cast<int64_t>(colbufs[i].i8.size()), colbufs[i].valid.data());
          break;
        }
        case CK_I32: {
          auto* nb = static_cast<arrow::Int32Builder*>(b.get());
          nb->AppendValues(colbufs[i].i32.data(), static_cast<int64_t>(colbufs[i].i32.size()), colbufs[i].valid.data());
          break;
        }
        case CK_I64: {
          auto* nb = static_cast<arrow::Int64Builder*>(b.get());
          nb->AppendValues(colbufs[i].i64.data(), static_cast<int64_t>(colbufs[i].i64.size()), colbufs[i].valid.data());
          break;
        }
        case CK_F32: {
          auto* nb = static_cast<arrow::FloatBuilder*>(b.get());
          nb->AppendValues(colbufs[i].f32.data(), static_cast<int64_t>(colbufs[i].f32.size()), colbufs[i].valid.data());
          break;
        }
        case CK_BOOL: {
          auto* nb = static_cast<arrow::BooleanBuilder*>(b.get());
          nb->AppendValues(colbufs[i].b8.data(), static_cast<int64_t>(colbufs[i].b8.size()), colbufs[i].valid.data());
          break;
        }
        case CK_STR: {
          auto* sb = static_cast<arrow::StringBuilder*>(b.get());
          int64_t rows = static_cast<int64_t>(colbufs[i].valid.size());
          int64_t total_bytes = static_cast<int64_t>(colbufs[i].sdata.size());
          sb->Reserve(rows);
          sb->ReserveData(total_bytes);
          for (int64_t r = 0; r < rows; ++r) {
            if (colbufs[i].valid[r]) {
              int32_t b0 = colbufs[i].soff[r];
              int32_t b1 = colbufs[i].soff[r+1];
              sb->Append(reinterpret_cast<const char*>(colbufs[i].sdata.data() + b0), b1 - b0);
            } else {
              sb->AppendNull();
            }
          }
          break;
        }
        case CK_LIST_I8:
        case CK_LIST_I32:
        case CK_LIST_I64:
        case CK_LIST_F32:
        case CK_LIST_BOOL:
        case CK_LIST_STR: {
          auto* lb = static_cast<arrow::ListBuilder*>(b.get());
          int64_t rows = static_cast<int64_t>(colbufs[i].valid.size());
          // get child builder
          switch (colbufs[i].kind) {
            case CK_LIST_I8: {
              auto* vb = static_cast<arrow::Int8Builder*>(lb->value_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].l_off[r]; int32_t b1 = colbufs[i].l_off[r+1];
                if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; }
                lb->Append(true);
                vb->AppendValues(colbufs[i].l_i8.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_LIST_I32: {
              auto* vb = static_cast<arrow::Int32Builder*>(lb->value_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].l_off[r]; int32_t b1 = colbufs[i].l_off[r+1];
                if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; }
                lb->Append(true);
                vb->AppendValues(colbufs[i].l_i32.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_LIST_I64: {
              auto* vb = static_cast<arrow::Int64Builder*>(lb->value_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].l_off[r]; int32_t b1 = colbufs[i].l_off[r+1];
                if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; }
                lb->Append(true);
                vb->AppendValues(colbufs[i].l_i64.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_LIST_F32: {
              auto* vb = static_cast<arrow::FloatBuilder*>(lb->value_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].l_off[r]; int32_t b1 = colbufs[i].l_off[r+1];
                if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; }
                lb->Append(true);
                vb->AppendValues(colbufs[i].l_f32.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_LIST_BOOL: {
              auto* vb = static_cast<arrow::BooleanBuilder*>(lb->value_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].l_off[r]; int32_t b1 = colbufs[i].l_off[r+1];
                if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; }
                lb->Append(true);
                vb->AppendValues(colbufs[i].l_b8.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_LIST_STR: {
              auto* vb = static_cast<arrow::StringBuilder*>(lb->value_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t e0 = colbufs[i].l_off[r]; int32_t e1 = colbufs[i].l_off[r+1];
                if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; }
                lb->Append(true);
                for (int32_t e = e0; e < e1; ++e) {
                  int32_t s0 = colbufs[i].l_soff[e]; int32_t s1 = colbufs[i].l_soff[e+1];
                  vb->Append(reinterpret_cast<const char*>(colbufs[i].l_sdata.data() + s0), s1 - s0);
                }
              }
              break;
            }
            default: break;
          }
          break;
        }
        case CK_MAP_STR_I64:
        case CK_MAP_STR_F32:
        case CK_MAP_STR_STR: {
          auto* mb = static_cast<arrow::MapBuilder*>(b.get());
          int64_t rows = static_cast<int64_t>(colbufs[i].valid.size());
          auto* kb = static_cast<arrow::StringBuilder*>(mb->key_builder());
          switch (colbufs[i].kind) {
            case CK_MAP_STR_I64: {
              auto* vb = static_cast<arrow::Int64Builder*>(mb->item_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].m_off[r]; int32_t b1 = colbufs[i].m_off[r+1];
                if (!colbufs[i].valid[r]) { mb->AppendNull(); continue; }
                mb->Append();
                // keys
                for (int32_t e = b0; e < b1; ++e) {
                  int32_t k0 = colbufs[i].mk_soff[e]; int32_t k1 = colbufs[i].mk_soff[e+1];
                  kb->Append(reinterpret_cast<const char*>(colbufs[i].mk_sdata.data() + k0), k1 - k0);
                }
                // values
                vb->AppendValues(colbufs[i].mv_i64.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_MAP_STR_F32: {
              auto* vb = static_cast<arrow::FloatBuilder*>(mb->item_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].m_off[r]; int32_t b1 = colbufs[i].m_off[r+1];
                if (!colbufs[i].valid[r]) { mb->AppendNull(); continue; }
                mb->Append();
                for (int32_t e = b0; e < b1; ++e) {
                  int32_t k0 = colbufs[i].mk_soff[e]; int32_t k1 = colbufs[i].mk_soff[e+1];
                  kb->Append(reinterpret_cast<const char*>(colbufs[i].mk_sdata.data() + k0), k1 - k0);
                }
                vb->AppendValues(colbufs[i].mv_f32.data() + b0, b1 - b0);
              }
              break;
            }
            case CK_MAP_STR_STR: {
              auto* vb = static_cast<arrow::StringBuilder*>(mb->item_builder());
              for (int64_t r = 0; r < rows; ++r) {
                int32_t b0 = colbufs[i].m_off[r]; int32_t b1 = colbufs[i].m_off[r+1];
                if (!colbufs[i].valid[r]) { mb->AppendNull(); continue; }
                mb->Append();
                for (int32_t e = b0; e < b1; ++e) {
                  int32_t k0 = colbufs[i].mk_soff[e]; int32_t k1 = colbufs[i].mk_soff[e+1];
                  kb->Append(reinterpret_cast<const char*>(colbufs[i].mk_sdata.data() + k0), k1 - k0);
                  int32_t v0 = colbufs[i].mv_soff[e]; int32_t v1 = colbufs[i].mv_soff[e+1];
                  vb->Append(reinterpret_cast<const char*>(colbufs[i].mv_sdata.data() + v0), v1 - v0);
                }
              }
              break;
            }
            default: break;
          }
          break;
        }
        case CK_NONE: {
          // already appended row-wise
          break;
        }
      }
    }
  }

  // Finish arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto& b = builders[i];
    std::shared_ptr<arrow::Array> a; b->Finish(&a); arrays.push_back(std::move(a));
  }
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(n), arrays);

  // Export via C Data
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*schema.get(), schema_out);
  arrow::ExportRecordBatch(*batch.get(), array_out, schema_out);
}

// Direct ByteBuffer[] version: payloads provided as direct buffers (sliced to exact bytes)
// Temporarily disable the complex direct-buffer decoder while we land deep-nesting support cleanly
#if 0
extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeThriftDecoder_decodeFromDirect(
    JNIEnv* env, jclass,
    jobjectArray jbuffers, jstring jpath, jstring jstruct,
    jlong schema_out_addr, jlong array_out_addr) {
  const char* cpath = env->GetStringUTFChars(jpath, nullptr);
  const char* cstruct = env->GetStringUTFChars(jstruct, nullptr);
  std::string thrift_path(cpath ? cpath : "");
  std::string struct_name(cstruct ? cstruct : "");
  if (cpath) env->ReleaseStringUTFChars(jpath, cpath);
  if (cstruct) env->ReleaseStringUTFChars(jstruct, cstruct);

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

  jsize n = env->GetArrayLength(jbuffers);

  // Columnar buffers as in array[] path
  enum ColKind {
    CK_NONE,
    CK_I8, CK_I32, CK_I64, CK_F32, CK_BOOL, CK_STR,
    CK_LIST_I8, CK_LIST_I32, CK_LIST_I64, CK_LIST_F32, CK_LIST_BOOL, CK_LIST_STR,
    CK_MAP_STR_I64, CK_MAP_STR_F32, CK_MAP_STR_STR
  };
  struct ColBuffers {
    ColKind kind{CK_NONE};
    std::vector<int8_t>  i8; std::vector<int32_t> i32; std::vector<int64_t> i64; std::vector<float> f32; std::vector<uint8_t> b8; std::vector<uint8_t> valid;
    std::vector<uint8_t> sdata; std::vector<int32_t> soff;
    std::vector<int32_t> l_off; std::vector<int8_t> l_i8; std::vector<int32_t> l_i32; std::vector<int64_t> l_i64; std::vector<float> l_f32; std::vector<uint8_t> l_b8; std::vector<uint8_t> l_sdata; std::vector<int32_t> l_soff;
    std::vector<int32_t> m_off; std::vector<uint8_t> mk_sdata; std::vector<int32_t> mk_soff; std::vector<int64_t> mv_i64; std::vector<float> mv_f32; std::vector<uint8_t> mv_sdata; std::vector<int32_t> mv_soff;
  };
  std::vector<ColBuffers> colbufs(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto tid = schema->field(i)->type()->id();
    ColKind k = CK_NONE;
    switch (tid) {
      case arrow::Type::INT8:   k = CK_I8; break;
      case arrow::Type::INT16:  k = CK_I32; break;
      case arrow::Type::INT32:  k = CK_I32; break;
      case arrow::Type::INT64:  k = CK_I64; break;
      case arrow::Type::FLOAT:  k = CK_F32; break;
      case arrow::Type::BOOL:   k = CK_BOOL; break;
      case arrow::Type::STRING: k = CK_STR; break;
      case arrow::Type::LIST: {
        auto lt = std::static_pointer_cast<arrow::ListType>(schema->field(i)->type());
        switch (lt->value_type()->id()) {
          case arrow::Type::INT8:  k = CK_LIST_I8; break;
          case arrow::Type::INT16:
          case arrow::Type::INT32: k = CK_LIST_I32; break;
          case arrow::Type::INT64: k = CK_LIST_I64; break;
          case arrow::Type::FLOAT: k = CK_LIST_F32; break;
          case arrow::Type::BOOL:  k = CK_LIST_BOOL; break;
          case arrow::Type::STRING: k = CK_LIST_STR; break;
          default: k = CK_NONE; break;
        }
        break;
      }
      case arrow::Type::MAP: {
        auto mt = std::static_pointer_cast<arrow::MapType>(schema->field(i)->type());
        if (mt->key_type()->id() == arrow::Type::STRING) {
          switch (mt->item_type()->id()) {
            case arrow::Type::INT64: k = CK_MAP_STR_I64; break;
            case arrow::Type::FLOAT: k = CK_MAP_STR_F32; break;
            case arrow::Type::STRING: k = CK_MAP_STR_STR; break;
            default: k = CK_NONE; break;
          }
        }
        break;
      }
      default: k = CK_NONE; break;
    }
    colbufs[i].kind = k;
    if (k != CK_NONE) {
      colbufs[i].valid.reserve(n);
      if (k == CK_STR) { colbufs[i].soff.reserve(n + 1); colbufs[i].soff.push_back(0); }
      if (k == CK_LIST_I8 || k == CK_LIST_I32 || k == CK_LIST_I64 || k == CK_LIST_F32 || k == CK_LIST_BOOL || k == CK_LIST_STR) { colbufs[i].l_off.reserve(n + 1); colbufs[i].l_off.push_back(0); if (k == CK_LIST_STR) { colbufs[i].l_soff.push_back(0); }}
      if (k == CK_MAP_STR_I64 || k == CK_MAP_STR_F32 || k == CK_MAP_STR_STR) { colbufs[i].m_off.reserve(n + 1); colbufs[i].m_off.push_back(0); colbufs[i].mk_soff.reserve(n + 1); colbufs[i].mk_soff.push_back(0); if (k == CK_MAP_STR_STR) { colbufs[i].mv_soff.reserve(n + 1); colbufs[i].mv_soff.push_back(0); } }
    }
  }

  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  builders.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) { std::unique_ptr<arrow::ArrayBuilder> b; arrow::MakeBuilder(arrow::default_memory_pool(), schema->field(i)->type(), &b); if (b) b->Reserve(static_cast<int64_t>(n)); builders.push_back(std::move(b)); }

  const int kBatch = 4000;
  auto mem = std::make_shared<TMemoryBuffer>();
  TBinaryProtocol prot(mem);
  std::vector<uint8_t> present(static_cast<size_t>(schema->num_fields()), 0);

  for (jsize start = 0; start < n; start += kBatch) {
    jsize end = std::min<jsize>(n, start + kBatch);
    for (int i = 0; i < schema->num_fields(); ++i) {
      if (colbufs[i].kind != CK_NONE) {
        switch (colbufs[i].kind) {
          case CK_STR: colbufs[i].sdata.clear(); colbufs[i].soff.clear(); colbufs[i].soff.push_back(0); colbufs[i].valid.clear(); break;
          case CK_I8: colbufs[i].i8.clear(); colbufs[i].valid.clear(); break;
          case CK_I32: colbufs[i].i32.clear(); colbufs[i].valid.clear(); break;
          case CK_I64: colbufs[i].i64.clear(); colbufs[i].valid.clear(); break;
          case CK_F32: colbufs[i].f32.clear(); colbufs[i].valid.clear(); break;
          case CK_BOOL: colbufs[i].b8.clear(); colbufs[i].valid.clear(); break;
          case CK_LIST_I8: case CK_LIST_I32: case CK_LIST_I64: case CK_LIST_F32: case CK_LIST_BOOL:
            colbufs[i].l_off.clear(); colbufs[i].l_off.push_back(0); colbufs[i].l_i8.clear(); colbufs[i].l_i32.clear(); colbufs[i].l_i64.clear(); colbufs[i].l_f32.clear(); colbufs[i].l_b8.clear(); colbufs[i].valid.clear(); break;
          case CK_LIST_STR:
            colbufs[i].l_off.clear(); colbufs[i].l_off.push_back(0); colbufs[i].l_sdata.clear(); colbufs[i].l_soff.clear(); colbufs[i].l_soff.push_back(0); colbufs[i].valid.clear(); break;
          case CK_MAP_STR_I64: case CK_MAP_STR_F32: case CK_MAP_STR_STR:
            colbufs[i].m_off.clear(); colbufs[i].m_off.push_back(0); colbufs[i].mk_sdata.clear(); colbufs[i].mk_soff.clear(); colbufs[i].mk_soff.push_back(0); colbufs[i].mv_i64.clear(); colbufs[i].mv_f32.clear(); colbufs[i].mv_sdata.clear(); colbufs[i].mv_soff.clear(); if (colbufs[i].kind == CK_MAP_STR_STR) colbufs[i].mv_soff.push_back(0); colbufs[i].valid.clear(); break;
          default: break;
        }
      }
    }

    // Parallel decode for direct ByteBuffers when requested
    int nthr = 1;
    if (const char* envthr = std::getenv("SURF_THRIFT_DECODE_THREADS")) {
      int v = std::atoi(envthr); if (v > 0) nthr = v;
    } else {
      unsigned hc = std::thread::hardware_concurrency();
      nthr = (hc > 0) ? std::min<unsigned>(hc, 8u) : 4;
    }
    // Disable parallel if complex list<struct> or map<string, struct> exist in schema
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto t = schema->field(i)->type();
      if (t->id() == arrow::Type::LIST) {
        auto lt = std::static_pointer_cast<arrow::ListType>(t);
        if (lt->value_type()->id() == arrow::Type::STRUCT) { nthr = 1; break; }
      } else if (t->id() == arrow::Type::MAP) {
        auto mt = std::static_pointer_cast<arrow::MapType>(t);
        if (mt->key_type()->id()==arrow::Type::STRING && mt->item_type()->id()==arrow::Type::STRUCT) { nthr = 1; break; }
      }
    }
    if (nthr > 1) {
      struct Input { const uint8_t* p; uint32_t len; };
      std::vector<Input> inputs; inputs.reserve(end - start);
      for (jsize r = start; r < end; ++r) {
        jobject buf = env->GetObjectArrayElement(jbuffers, r);
        if (!buf) { inputs.push_back({nullptr, 0}); continue; }
        void* base = env->GetDirectBufferAddress(buf);
        jlong cap = env->GetDirectBufferCapacity(buf);
        inputs.push_back({reinterpret_cast<const uint8_t*>(base), static_cast<uint32_t>(cap)});
        env->DeleteLocalRef(buf);
      }
      int rows = static_cast<int>(inputs.size());
      nthr = std::min(nthr, std::max(1, rows));
      int chunk = (rows + nthr - 1) / nthr;
      // Prepare results per chunk
      struct ChunkRes { std::vector<ColBuffers> cols; int rows{0}; };
      std::vector<ChunkRes> results(nthr);
      // Precompute kinds across schema
      std::vector<ColKind> kinds(schema->num_fields());
      for (int i = 0; i < schema->num_fields(); ++i) kinds[i] = colbufs[i].kind;
      auto worker = [&](int tid){
        int s = tid * chunk; int e = std::min(rows, s + chunk); if (s >= e) return;
        auto mem_local = std::make_shared<TMemoryBuffer>();
        TBinaryProtocol prot_local(mem_local);
        ChunkRes out; out.cols.resize(schema->num_fields());
        for (int i = 0; i < schema->num_fields(); ++i) {
          out.cols[i].kind = kinds[i];
          if (kinds[i] != CK_NONE) {
            if (kinds[i] == CK_STR) { out.cols[i].soff.push_back(0); }
            if (kinds[i] == CK_LIST_I8 || kinds[i] == CK_LIST_I32 || kinds[i] == CK_LIST_I64 || kinds[i] == CK_LIST_F32 || kinds[i] == CK_LIST_BOOL || kinds[i] == CK_LIST_STR) {
              out.cols[i].l_off.push_back(0);
              if (kinds[i] == CK_LIST_STR) out.cols[i].l_soff.push_back(0);
            }
            if (kinds[i] == CK_MAP_STR_I64 || kinds[i] == CK_MAP_STR_F32 || kinds[i] == CK_MAP_STR_STR) {
              out.cols[i].m_off.push_back(0); out.cols[i].mk_soff.push_back(0); if (kinds[i] == CK_MAP_STR_STR) out.cols[i].mv_soff.push_back(0);
            }
          }
        }
        std::vector<uint8_t> present_local(static_cast<size_t>(schema->num_fields()), 0);
        for (int r = s; r < e; ++r) {
          const auto& in = inputs[r]; if (!in.p || in.len == 0) continue;
          mem_local->resetBuffer(const_cast<uint8_t*>(in.p), in.len);
          std::fill(present_local.begin(), present_local.end(), 0);
          std::string sname; prot_local.readStructBegin(sname);
          while (true) {
            std::string fname; TType ftype; int16_t fid; prot_local.readFieldBegin(fname, ftype, fid);
            if (ftype == TType::T_STOP) break;
            auto it = id_to_index.find(fid);
            if (it == id_to_index.end()) { prot_local.skip(ftype); prot_local.readFieldEnd(); continue; }
            int idx = it->second; auto colk = kinds[idx]; auto& cb = out.cols[idx];
            switch (schema->field(idx)->type()->id()) {
              case arrow::Type::NA: { present_local[idx] = 1; break; }
              case arrow::Type::BOOL: {
                if (colk == CK_BOOL && ftype == TType::T_BOOL) { bool v; prot_local.readBool(v); cb.b8.push_back(v?1:0); cb.valid.push_back(1); present_local[idx]=1; }
                else { present_local[idx]=1; prot_local.skip(ftype); }
                break;
              }
              case arrow::Type::INT8: {
                if (colk == CK_I8 && ftype == TType::T_BYTE) { int8_t v; prot_local.readByte(v); cb.i8.push_back(v); cb.valid.push_back(1); present_local[idx]=1; }
                else { present_local[idx]=1; prot_local.skip(ftype); }
                break;
              }
              case arrow::Type::INT16:
              case arrow::Type::INT32: {
                if (colk == CK_I32 && (ftype == TType::T_I16 || ftype == TType::T_I32)) { if (ftype==TType::T_I16){ int16_t t; prot_local.readI16(t); cb.i32.push_back(static_cast<int32_t>(t)); } else { int32_t t; prot_local.readI32(t); cb.i32.push_back(t);} cb.valid.push_back(1); present_local[idx]=1; }
                else { present_local[idx]=1; prot_local.skip(ftype); }
                break;
              }
              case arrow::Type::INT64: {
                if (colk == CK_I64 && ftype == TType::T_I64) { int64_t v; prot_local.readI64(v); cb.i64.push_back(v); cb.valid.push_back(1); present_local[idx]=1; }
                else { present_local[idx]=1; prot_local.skip(ftype); }
                break;
              }
              case arrow::Type::FLOAT: {
                if (colk == CK_F32 && ftype == TType::T_DOUBLE) { double dv; prot_local.readDouble(dv); cb.f32.push_back(static_cast<float>(dv)); cb.valid.push_back(1); present_local[idx]=1; }
                else { present_local[idx]=1; prot_local.skip(ftype); }
                break;
              }
              case arrow::Type::STRING: {
                if (colk == CK_STR && ftype == TType::T_STRING) {
                  int32_t len = 0; prot_local.readI32(len);
                  if (len > 0) {
                    uint32_t want = static_cast<uint32_t>(len);
                    const uint8_t* p = mem_local->borrow(nullptr, &want);
                    if (p && want >= static_cast<uint32_t>(len)) { cb.sdata.insert(cb.sdata.end(), p, p + len); cb.soff.push_back(cb.soff.back() + len); mem_local->consume(static_cast<uint32_t>(len)); }
                    else { std::vector<uint8_t> tmp(len); uint32_t got=0; while (got < (uint32_t)len) { uint32_t rr = mem_local->read(tmp.data()+got, (uint32_t)len-got); if (rr==0) break; got+=rr; } cb.sdata.insert(cb.sdata.end(), tmp.begin(), tmp.end()); cb.soff.push_back(cb.soff.back()+len); }
                  } else { cb.soff.push_back(cb.soff.back()); }
                  cb.valid.push_back(1); present_local[idx]=1;
                } else { present_local[idx]=1; prot_local.skip(ftype); }
                break;
              }
              case arrow::Type::LIST: {
                switch (colk) {
                  case CK_LIST_I8: case CK_LIST_I32: case CK_LIST_I64: case CK_LIST_F32: case CK_LIST_BOOL: case CK_LIST_STR: {
                    if (ftype == TType::T_LIST) {
                      apache::thrift::protocol::TType et; uint32_t sz; prot_local.readListBegin(et, sz);
                      if (colk == CK_LIST_I8 && et == TType::T_BYTE) { for (uint32_t j=0;j<sz;++j){int8_t v;prot_local.readByte(v);cb.l_i8.push_back(v);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+ (int32_t)sz); }
                      else if (colk == CK_LIST_I32 && (et == TType::T_I16 || et == TType::T_I32)) { for (uint32_t j=0;j<sz;++j){ if (et==TType::T_I16){int16_t v;prot_local.readI16(v);cb.l_i32.push_back((int32_t)v);} else {int32_t v;prot_local.readI32(v);cb.l_i32.push_back(v);} } cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+ (int32_t)sz); }
                      else if (colk == CK_LIST_I64 && et == TType::T_I64) { for (uint32_t j=0;j<sz;++j){int64_t v;prot_local.readI64(v);cb.l_i64.push_back(v);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+ (int32_t)sz); }
                      else if (colk == CK_LIST_F32 && et == TType::T_DOUBLE) { for (uint32_t j=0;j<sz;++j){double dv;prot_local.readDouble(dv);cb.l_f32.push_back((float)dv);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+ (int32_t)sz); }
                      else if (colk == CK_LIST_BOOL && et == TType::T_BOOL) { for (uint32_t j=0;j<sz;++j){bool v;prot_local.readBool(v);cb.l_b8.push_back(v?1:0);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+ (int32_t)sz); }
                      else if (colk == CK_LIST_STR && et == TType::T_STRING) { for (uint32_t j=0;j<sz;++j){ int32_t slen=0; prot_local.readI32(slen); if (slen>0){ uint32_t want=(uint32_t)slen; const uint8_t* p = mem_local->borrow(nullptr,&want); if (p && want >= (uint32_t)slen){ cb.l_sdata.insert(cb.l_sdata.end(), p, p+slen); cb.l_soff.push_back(cb.l_soff.back()+slen); mem_local->consume((uint32_t)slen);} else { std::vector<uint8_t> tmp(slen); uint32_t got=0; while (got < (uint32_t)slen){ uint32_t rr = mem_local->read(tmp.data()+got, (uint32_t)slen-got); if (rr==0) break; got+=rr; } cb.l_sdata.insert(cb.l_sdata.end(), tmp.begin(), tmp.end()); cb.l_soff.push_back(cb.l_soff.back()+slen);} } else { cb.l_soff.push_back(cb.l_soff.back()); } } cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+ (int32_t)sz); }
                      else { prot_local.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                      prot_local.readListEnd(); present_local[idx]=1;
                    } else { present_local[idx]=1; }
                    break;
                  }
                  default: { prot_local.skip(ftype); present_local[idx]=1; break; }
                }
                break;
              }
              case arrow::Type::MAP: {
                switch (colk) {
                  case CK_MAP_STR_I64: case CK_MAP_STR_F32: case CK_MAP_STR_STR: {
                    if (ftype == TType::T_MAP) {
                      apache::thrift::protocol::TType kt, vt; uint32_t sz; prot_local.readMapBegin(kt, vt, sz);
                      if (kt == TType::T_STRING) {
                        for (uint32_t j=0;j<sz;++j){ int32_t klen=0; prot_local.readI32(klen); if (klen>0){ uint32_t want=(uint32_t)klen; const uint8_t* p=mem_local->borrow(nullptr,&want); if (p && want>=(uint32_t)klen){ cb.mk_sdata.insert(cb.mk_sdata.end(), p, p+klen); cb.mk_soff.push_back(cb.mk_soff.back()+klen); mem_local->consume((uint32_t)klen);} else { std::vector<uint8_t> tmp(klen); uint32_t got=0; while (got<(uint32_t)klen){ uint32_t rr=mem_local->read(tmp.data()+got, (uint32_t)klen-got); if (rr==0) break; got+=rr;} cb.mk_sdata.insert(cb.mk_sdata.end(), tmp.begin(), tmp.end()); cb.mk_soff.push_back(cb.mk_soff.back()+klen);} }
                          switch (colk){ case CK_MAP_STR_I64: { int64_t v; prot_local.readI64(v); cb.mv_i64.push_back(v); break; } case CK_MAP_STR_F32: { double dv; prot_local.readDouble(dv); cb.mv_f32.push_back((float)dv); break; } case CK_MAP_STR_STR: { int32_t vlen=0; prot_local.readI32(vlen); if (vlen>0){ uint32_t want=(uint32_t)vlen; const uint8_t* p2=mem_local->borrow(nullptr,&want); if (p2 && want>=(uint32_t)vlen){ cb.mv_sdata.insert(cb.mv_sdata.end(), p2, p2+vlen); cb.mv_soff.push_back(cb.mv_soff.back()+vlen); mem_local->consume((uint32_t)vlen);} else { std::vector<uint8_t> tmp2(vlen); uint32_t got2=0; while (got2<(uint32_t)vlen){ uint32_t rr2=mem_local->read(tmp2.data()+got2, (uint32_t)vlen-got2); if (rr2==0) break; got2+=rr2;} cb.mv_sdata.insert(cb.mv_sdata.end(), tmp2.begin(), tmp2.end()); cb.mv_soff.push_back(cb.mv_soff.back()+vlen);} } else { cb.mv_soff.push_back(cb.mv_soff.back()); } break; } default: break; }
                        }
                        cb.valid.push_back(1); cb.m_off.push_back(cb.m_off.back() + (int32_t)sz);
                      } else { prot_local.skip(vt); cb.valid.push_back(0); cb.m_off.push_back(cb.m_off.back()); }
                      prot_local.readMapEnd(); present_local[idx]=1;
                    } else { present_local[idx]=1; }
                    break;
                  }
                  default: { prot_local.skip(ftype); present_local[idx]=1; break; }
                }
                break;
              }
              default: { prot_local.skip(ftype); present_local[idx]=1; break; }
            }
            prot_local.readFieldEnd();
          }
          prot_local.readStructEnd();
          // Placeholders for missing fields
          for (int i = 0; i < schema->num_fields(); ++i) if (!present_local[i]) {
            switch (kinds[i]) {
              case CK_I8: out.cols[i].i8.push_back(0); out.cols[i].valid.push_back(0); break;
              case CK_I32: out.cols[i].i32.push_back(0); out.cols[i].valid.push_back(0); break;
              case CK_I64: out.cols[i].i64.push_back(0); out.cols[i].valid.push_back(0); break;
              case CK_F32: out.cols[i].f32.push_back(0.0f); out.cols[i].valid.push_back(0); break;
              case CK_BOOL: out.cols[i].b8.push_back(0); out.cols[i].valid.push_back(0); break;
              case CK_STR: out.cols[i].soff.push_back(out.cols[i].soff.back()); out.cols[i].valid.push_back(0); break;
              default: break;
            }
          }
        }
        out.rows = e - s;
        results[tid] = std::move(out);
      };
      std::vector<std::thread> ths; ths.reserve(nthr);
      for (int t = 0; t < nthr; ++t) ths.emplace_back(worker, t);
      for (auto& th : ths) th.join();
      // Flush chunks in order to builders
      for (int t = 0; t < nthr; ++t) {
        const auto& out = results[t]; if (out.rows <= 0) continue;
        for (int i = 0; i < schema->num_fields(); ++i) {
          auto& b = builders[i]; const auto& cb = out.cols[i];
          switch (cb.kind) {
            case CK_I8: { auto* nb = static_cast<arrow::Int8Builder*>(b.get()); nb->AppendValues(cb.i8.data(), (int64_t)cb.i8.size(), cb.valid.data()); break; }
            case CK_I32: { auto* nb = static_cast<arrow::Int32Builder*>(b.get()); nb->AppendValues(cb.i32.data(), (int64_t)cb.i32.size(), cb.valid.data()); break; }
            case CK_I64: { auto* nb = static_cast<arrow::Int64Builder*>(b.get()); nb->AppendValues(cb.i64.data(), (int64_t)cb.i64.size(), cb.valid.data()); break; }
            case CK_F32: { auto* nb = static_cast<arrow::FloatBuilder*>(b.get()); nb->AppendValues(cb.f32.data(), (int64_t)cb.f32.size(), cb.valid.data()); break; }
            case CK_BOOL: { auto* nb = static_cast<arrow::BooleanBuilder*>(b.get()); nb->AppendValues(cb.b8.data(), (int64_t)cb.b8.size(), cb.valid.data()); break; }
            case CK_STR: { auto* sb = static_cast<arrow::StringBuilder*>(b.get()); int64_t rowsL = (int64_t)cb.valid.size(); int64_t total_bytes = (int64_t)cb.sdata.size(); sb->Reserve(rowsL); sb->ReserveData(total_bytes); for (int64_t r=0;r<rowsL;++r){ if (cb.valid[r]){ int32_t b0=cb.soff[r], b1=cb.soff[r+1]; sb->Append(reinterpret_cast<const char*>(cb.sdata.data()+b0), b1-b0);} else { sb->AppendNull(); } } break; }
            case CK_LIST_I8: case CK_LIST_I32: case CK_LIST_I64: case CK_LIST_F32: case CK_LIST_BOOL: case CK_LIST_STR: {
              auto* lb = static_cast<arrow::ListBuilder*>(b.get()); int64_t rowsL = (int64_t)cb.valid.size();
              switch (cb.kind) {
                case CK_LIST_I8: { auto* vb = static_cast<arrow::Int8Builder*>(lb->value_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.l_off[r], z=cb.l_off[r+1]; if (!cb.valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(cb.l_i8.data()+a, z-a);} break; }
                case CK_LIST_I32: { auto* vb = static_cast<arrow::Int32Builder*>(lb->value_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.l_off[r], z=cb.l_off[r+1]; if (!cb.valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(cb.l_i32.data()+a, z-a);} break; }
                case CK_LIST_I64: { auto* vb = static_cast<arrow::Int64Builder*>(lb->value_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.l_off[r], z=cb.l_off[r+1]; if (!cb.valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(cb.l_i64.data()+a, z-a);} break; }
                case CK_LIST_F32: { auto* vb = static_cast<arrow::FloatBuilder*>(lb->value_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.l_off[r], z=cb.l_off[r+1]; if (!cb.valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(cb.l_f32.data()+a, z-a);} break; }
                case CK_LIST_BOOL: { auto* vb = static_cast<arrow::BooleanBuilder*>(lb->value_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.l_off[r], z=cb.l_off[r+1]; if (!cb.valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(cb.l_b8.data()+a, z-a);} break; }
                case CK_LIST_STR: { auto* vb = static_cast<arrow::StringBuilder*>(lb->value_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t e0=cb.l_off[r], e1=cb.l_off[r+1]; if (!cb.valid[r]) { lb->AppendNull(); continue; } lb->Append(true); for (int32_t e=e0;e<e1;++e){ int32_t s0=cb.l_soff[e], s1=cb.l_soff[e+1]; vb->Append(reinterpret_cast<const char*>(cb.l_sdata.data()+s0), s1-s0);} } break; }
                default: break;
              }
              break;
            }
            case CK_MAP_STR_I64: case CK_MAP_STR_F32: case CK_MAP_STR_STR: {
              auto* mb = static_cast<arrow::MapBuilder*>(b.get()); int64_t rowsL = (int64_t)cb.valid.size(); auto* kb = static_cast<arrow::StringBuilder*>(mb->key_builder());
              switch (cb.kind) {
                case CK_MAP_STR_I64: { auto* vb = static_cast<arrow::Int64Builder*>(mb->item_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.m_off[r], z=cb.m_off[r+1]; if (!cb.valid[r]) { mb->AppendNull(); continue; } mb->Append(); for (int32_t e=a;e<z;++e){ int32_t k0=cb.mk_soff[e], k1=cb.mk_soff[e+1]; kb->Append(reinterpret_cast<const char*>(cb.mk_sdata.data()+k0), k1-k0);} vb->AppendValues(cb.mv_i64.data()+a, z-a);} break; }
                case CK_MAP_STR_F32: { auto* vb = static_cast<arrow::FloatBuilder*>(mb->item_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.m_off[r], z=cb.m_off[r+1]; if (!cb.valid[r]) { mb->AppendNull(); continue; } mb->Append(); for (int32_t e=a;e<z;++e){ int32_t k0=cb.mk_soff[e], k1=cb.mk_soff[e+1]; kb->Append(reinterpret_cast<const char*>(cb.mk_sdata.data()+k0), k1-k0);} vb->AppendValues(cb.mv_f32.data()+a, z-a);} break; }
                case CK_MAP_STR_STR: { auto* vb = static_cast<arrow::StringBuilder*>(mb->item_builder()); for (int64_t r=0;r<rowsL;++r){ int32_t a=cb.m_off[r], z=cb.m_off[r+1]; if (!cb.valid[r]) { mb->AppendNull(); continue; } mb->Append(); for (int32_t e=a;e<z;++e){ int32_t k0=cb.mk_soff[e], k1=cb.mk_soff[e+1]; kb->Append(reinterpret_cast<const char*>(cb.mk_sdata.data()+k0), k1-k0); int32_t v0=cb.mv_soff[e], v1=cb.mv_soff[e+1]; vb->Append(reinterpret_cast<const char*>(cb.mv_sdata.data()+v0), v1-v0);} } break; }
                default: break;
              }
              break;
            }
            default: break;
          }
        }
      }
      continue; // next micro-batch
    }

    // Helper: direct binary read into buffers (avoid temporary std::string)
    auto readBinaryAppend = [&](std::vector<uint8_t>& data, std::vector<int32_t>& offs) {
      int32_t len = 0; prot.readI32(len);
      if (len <= 0) { offs.push_back(offs.back()); return; }
      uint32_t want = static_cast<uint32_t>(len);
      const uint8_t* p = mem->borrow(nullptr, &want);
      if (p && want >= static_cast<uint32_t>(len)) {
        data.insert(data.end(), p, p + len);
        offs.push_back(offs.back() + len);
        mem->consume(static_cast<uint32_t>(len));
      } else {
        std::vector<uint8_t> tmp(len); uint32_t got = 0;
        while (got < static_cast<uint32_t>(len)) { uint32_t r = mem->read(tmp.data() + got, static_cast<uint32_t>(len) - got); if (r == 0) break; got += r; }
        data.insert(data.end(), tmp.begin(), tmp.end());
        offs.push_back(offs.back() + len);
      }
    };

    for (jsize r = start; r < end; ++r) {
      jobject buf = env->GetObjectArrayElement(jbuffers, r);
      if (!buf) continue;
      void* base = env->GetDirectBufferAddress(buf);
      jlong cap = env->GetDirectBufferCapacity(buf);
      if (!base || cap <= 0) { env->DeleteLocalRef(buf); continue; }
      mem->resetBuffer(reinterpret_cast<uint8_t*>(base), static_cast<uint32_t>(cap));
      std::memset(present.data(), 0, present.size());

      std::string sname; prot.readStructBegin(sname);
      while (true) {
        std::string fname; TType ftype; int16_t fid; prot.readFieldBegin(fname, ftype, fid);
        if (ftype == TType::T_STOP) break;
        auto it = id_to_index.find(fid);
        if (it == id_to_index.end()) { prot.skip(ftype); prot.readFieldEnd(); continue; }
        int idx = it->second; auto& b = builders[idx]; auto colk = colbufs[idx].kind;
        switch (schema->field(idx)->type()->id()) {
          case arrow::Type::NA: { prot.skip(ftype); b->AppendNull(); present[idx] = 1; break; }
          case arrow::Type::BOOL: {
            if (colk == CK_BOOL && ftype == TType::T_BOOL) { bool v; prot.readBool(v); colbufs[idx].b8.push_back(v?1:0); colbufs[idx].valid.push_back(1); present[idx]=1; }
            else { DecodeAndAppend(&prot, ftype, b.get()); present[idx]=1; }
            break;
          }
          case arrow::Type::INT8: {
            if (colk == CK_I8 && ftype == TType::T_BYTE) { int8_t v; prot.readByte(v); colbufs[idx].i8.push_back(v); colbufs[idx].valid.push_back(1); present[idx]=1; }
            else { DecodeAndAppend(&prot, ftype, b.get()); present[idx]=1; }
            break;
          }
          case arrow::Type::INT16: case arrow::Type::INT32: {
            if (colk == CK_I32 && (ftype == TType::T_I16 || ftype == TType::T_I32)) { int32_t outv=0; if (ftype==TType::T_I16){int16_t t;prot.readI16(t);outv=static_cast<int32_t>(t);} else {int32_t t;prot.readI32(t);outv=t;} colbufs[idx].i32.push_back(outv); colbufs[idx].valid.push_back(1); present[idx]=1; }
            else { DecodeAndAppend(&prot, ftype, b.get()); present[idx]=1; }
            break;
          }
          case arrow::Type::INT64: {
            if (colk == CK_I64 && ftype == TType::T_I64) { int64_t v; prot.readI64(v); colbufs[idx].i64.push_back(v); colbufs[idx].valid.push_back(1); present[idx]=1; }
            else { DecodeAndAppend(&prot, ftype, b.get()); present[idx]=1; }
            break;
          }
          case arrow::Type::FLOAT: {
            if (colk == CK_F32 && ftype == TType::T_DOUBLE) { double dv; prot.readDouble(dv); colbufs[idx].f32.push_back(static_cast<float>(dv)); colbufs[idx].valid.push_back(1); present[idx]=1; }
            else { DecodeAndAppend(&prot, ftype, b.get()); present[idx]=1; }
            break;
          }
          case arrow::Type::STRING: { if (colk == CK_STR && ftype == TType::T_STRING) { auto& cb = colbufs[idx]; readBinaryAppend(cb.sdata, cb.soff); cb.valid.push_back(1); present[idx]=1; } else { DecodeAndAppend(&prot, ftype, b.get()); present[idx]=1; } break; }
          case arrow::Type::LIST: {
            // Row-wise for list<struct>
            auto lt = std::static_pointer_cast<arrow::ListType>(schema->field(idx)->type());
            if (lt->value_type()->id() == arrow::Type::STRUCT && ftype == TType::T_LIST) {
              apache::thrift::protocol::TType et; uint32_t sz; prot.readListBegin(et, sz); auto* lb = static_cast<arrow::ListBuilder*>(b.get()); auto* sb = static_cast<arrow::StructBuilder*>(lb->value_builder()); lb->Append(true); std::string nested_name; if (idx < (int)top_field_irs.size() && top_field_irs[idx].kind==TFieldIR::LIST && top_field_irs[idx].list_elem) nested_name = top_field_irs[idx].list_elem->struct_name; auto itn = table.find(nested_name); std::unordered_map<int,int> nid2pos; if (itn!=table.end()) nid2pos = build_id_to_index(itn->second); if (et==TType::T_STRUCT){ for (uint32_t j=0;j<sz;++j){ std::string sn; prot.readStructBegin(sn); sb->Append(true); std::vector<uint8_t> child_present(sb->num_fields(),0); while (true){ std::string fn; TType ft2; int16_t fid; prot.readFieldBegin(fn, ft2, fid); if (ft2==TType::T_STOP) break; int cpos=-1; auto itp=nid2pos.find(fid); if (itp!=nid2pos.end()) cpos=itp->second; if (cpos>=0){ auto* cbb = sb->field_builder(cpos); switch (sb->type()->field(cpos)->type()->id()){ case arrow::Type::INT64: case arrow::Type::INT32: case arrow::Type::INT8: case arrow::Type::FLOAT: case arrow::Type::BOOL: { DecodeAndAppend(&prot, ft2, cbb); child_present[cpos]=1; break;} case arrow::Type::STRING: { if (ft2==TType::T_STRING){ std::string s; prot.readBinary(s); static_cast<arrow::StringBuilder*>(cbb)->Append(s); child_present[cpos]=1; } else { prot.skip(ft2);} break;} case arrow::Type::LIST: { auto clt = std::static_pointer_cast<arrow::ListType>(sb->type()->field(cpos)->type()); if (clt->value_type()->id()==arrow::Type::STRING && ft2==TType::T_LIST){ auto* lb2=static_cast<arrow::ListBuilder*>(cbb); auto* svb=static_cast<arrow::StringBuilder*>(lb2->value_builder()); apache::thrift::protocol::TType et2; uint32_t lsz; prot.readListBegin(et2, lsz); if (et2==TType::T_STRING){ lb2->Append(true); for (uint32_t t=0;t<lsz;++t){ std::string s; prot.readBinary(s); svb->Append(s);} } else { lb2->AppendNull(); prot.skip(et2);} prot.readListEnd(); child_present[cpos]=1; } else { prot.skip(ft2);} break; } default: prot.skip(ft2); break;} } else { prot.skip(ft2);} prot.readFieldEnd(); } prot.readStructEnd(); for (int cc=0; cc<sb->num_fields(); ++cc) if (!child_present[cc]) sb->field_builder(cc)->AppendNull(); } } else { prot.skip(et);} prot.readListEnd(); present[idx]=1; break; }
            }
            switch (colk) {
              case CK_LIST_I8: case CK_LIST_I32: case CK_LIST_I64: case CK_LIST_F32: case CK_LIST_BOOL: case CK_LIST_STR: {
                if (ftype == TType::T_LIST) {
                  apache::thrift::protocol::TType et; uint32_t sz; prot.readListBegin(et, sz); auto& cb = colbufs[idx];
                  if (colk == CK_LIST_I8 && et == TType::T_BYTE) { cb.l_i8.reserve(cb.l_i8.size()+sz); for (uint32_t j=0;j<sz;++j){int8_t v;prot.readByte(v);cb.l_i8.push_back(v);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+static_cast<int32_t>(sz)); }
                  else if (colk == CK_LIST_I32 && (et == TType::T_I16 || et == TType::T_I32)) { cb.l_i32.reserve(cb.l_i32.size()+sz); for (uint32_t j=0;j<sz;++j){ if (et==TType::T_I16){int16_t v;prot.readI16(v);cb.l_i32.push_back(static_cast<int32_t>(v));} else {int32_t v;prot.readI32(v);cb.l_i32.push_back(v);} } cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+static_cast<int32_t>(sz)); }
                  else if (colk == CK_LIST_I64 && et == TType::T_I64) { cb.l_i64.reserve(cb.l_i64.size()+sz); for (uint32_t j=0;j<sz;++j){int64_t v;prot.readI64(v);cb.l_i64.push_back(v);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+static_cast<int32_t>(sz)); }
                  else if (colk == CK_LIST_F32 && et == TType::T_DOUBLE) { cb.l_f32.reserve(cb.l_f32.size()+sz); for (uint32_t j=0;j<sz;++j){double dv;prot.readDouble(dv);cb.l_f32.push_back(static_cast<float>(dv));} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+static_cast<int32_t>(sz)); }
                  else if (colk == CK_LIST_BOOL && et == TType::T_BOOL) { cb.l_b8.reserve(cb.l_b8.size()+sz); for (uint32_t j=0;j<sz;++j){bool v;prot.readBool(v);cb.l_b8.push_back(v?1:0);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+static_cast<int32_t>(sz)); }
                  else if (colk == CK_LIST_STR && et == TType::T_STRING) { for (uint32_t j=0;j<sz;++j){ readBinaryAppend(cb.l_sdata, cb.l_soff);} cb.valid.push_back(1); cb.l_off.push_back(cb.l_off.back()+static_cast<int32_t>(sz)); }
                  else { prot.skip(et); cb.valid.push_back(0); cb.l_off.push_back(cb.l_off.back()); }
                  prot.readListEnd(); present[idx]=1;
                } else { present[idx]=1; }
                break;
              }
              default: prot.skip(ftype); b->AppendNull(); present[idx]=1; break;
            }
            break;
          }
          case arrow::Type::MAP: {
            // Row-wise map<string, struct>
            auto mt = std::static_pointer_cast<arrow::MapType>(schema->field(idx)->type());
            if (mt->key_type()->id()==arrow::Type::STRING && mt->item_type()->id()==arrow::Type::STRUCT && ftype==TType::T_MAP) {
              apache::thrift::protocol::TType kt, vt; uint32_t sz; prot.readMapBegin(kt, vt, sz); auto* mb=static_cast<arrow::MapBuilder*>(b.get()); auto* kb=static_cast<arrow::StringBuilder*>(mb->key_builder()); auto* sb=static_cast<arrow::StructBuilder*>(mb->item_builder()); std::string nested_name; if (idx < (int)top_field_irs.size() && top_field_irs[idx].kind==TFieldIR::MAP && top_field_irs[idx].map_val) nested_name = top_field_irs[idx].map_val->struct_name; auto itn=table.find(nested_name); std::unordered_map<int,int> nid2pos; if (itn!=table.end()) nid2pos = build_id_to_index(itn->second); if (kt==TType::T_STRING && vt==TType::T_STRUCT){ mb->Append(); for (uint32_t e=0;e<sz;++e){ std::string k; prot.readBinary(k); kb->Append(k); std::string sn; prot.readStructBegin(sn); sb->Append(true); std::vector<uint8_t> child_present(sb->num_fields(),0); while (true){ std::string fn; TType ft2; int16_t fid; prot.readFieldBegin(fn, ft2, fid); if (ft2==TType::T_STOP) break; int cpos=-1; auto itp = nid2pos.find(fid); if (itp!=nid2pos.end()) cpos=itp->second; if (cpos>=0){ auto* cbb=sb->field_builder(cpos); switch (sb->type()->field(cpos)->type()->id()){ case arrow::Type::INT64: case arrow::Type::INT32: case arrow::Type::INT8: case arrow::Type::FLOAT: case arrow::Type::BOOL: { DecodeAndAppend(&prot, ft2, cbb); child_present[cpos]=1; break;} case arrow::Type::STRING: { if (ft2==TType::T_STRING){ std::string s; prot.readBinary(s); static_cast<arrow::StringBuilder*>(cbb)->Append(s); child_present[cpos]=1;} else { prot.skip(ft2);} break;} case arrow::Type::LIST: { auto clt=std::static_pointer_cast<arrow::ListType>(sb->type()->field(cpos)->type()); if (clt->value_type()->id()==arrow::Type::STRING && ft2==TType::T_LIST){ auto* lb2=static_cast<arrow::ListBuilder*>(cbb); auto* svb=static_cast<arrow::StringBuilder*>(lb2->value_builder()); apache::thrift::protocol::TType et2; uint32_t lsz; prot.readListBegin(et2, lsz); if (et2==TType::T_STRING){ lb2->Append(true); for (uint32_t t=0;t<lsz;++t){ std::string s; prot.readBinary(s); svb->Append(s);} } else { lb2->AppendNull(); prot.skip(et2);} prot.readListEnd(); child_present[cpos]=1; } else { prot.skip(ft2);} break;} default: prot.skip(ft2); break;} } else { prot.skip(ft2);} prot.readFieldEnd(); } prot.readStructEnd(); for (int cc=0; cc<sb->num_fields(); ++cc) if (!child_present[cc]) sb->field_builder(cc)->AppendNull(); } } else { prot.skip(vt);} prot.readMapEnd(); present[idx]=1; break; }
            }
            switch (colk) {
              case CK_MAP_STR_I64: case CK_MAP_STR_F32: case CK_MAP_STR_STR: {
                if (ftype == TType::T_MAP) {
                  apache::thrift::protocol::TType kt, vt; uint32_t sz; prot.readMapBegin(kt, vt, sz); auto& cb = colbufs[idx];
                  if (kt == TType::T_STRING) {
                    for (uint32_t j=0;j<sz;++j){ std::string k; prot.readBinary(k); cb.mk_sdata.reserve(cb.mk_sdata.size()+k.size()); cb.mk_sdata.insert(cb.mk_sdata.end(), reinterpret_cast<const uint8_t*>(k.data()), reinterpret_cast<const uint8_t*>(k.data())+k.size()); cb.mk_soff.push_back(cb.mk_soff.back()+static_cast<int32_t>(k.size())); switch (colk){ case CK_MAP_STR_I64: {int64_t v;prot.readI64(v);cb.mv_i64.push_back(v);break;} case CK_MAP_STR_F32: {double dv;prot.readDouble(dv);cb.mv_f32.push_back(static_cast<float>(dv));break;} case CK_MAP_STR_STR: { std::string vs; prot.readBinary(vs); cb.mv_sdata.reserve(cb.mv_sdata.size()+vs.size()); cb.mv_sdata.insert(cb.mv_sdata.end(), reinterpret_cast<const uint8_t*>(vs.data()), reinterpret_cast<const uint8_t*>(vs.data())+vs.size()); cb.mv_soff.push_back(cb.mv_soff.back()+static_cast<int32_t>(vs.size())); break;} default: break;} }
                    cb.valid.push_back(1); cb.m_off.push_back(cb.m_off.back()+static_cast<int32_t>(sz));
                  } else { prot.skip(vt); cb.valid.push_back(0); cb.m_off.push_back(cb.m_off.back()); }
                  prot.readMapEnd(); present[idx]=1;
                } else { present[idx]=1; }
                break;
              }
              default: prot.skip(ftype); b->AppendNull(); present[idx]=1; break;
            }
            break;
          }
          default: prot.skip(ftype); b->AppendNull(); present[idx]=1; break;
        }
        prot.readFieldEnd();
      }
      prot.readStructEnd();
      for (int i = 0; i < schema->num_fields(); ++i) {
        if (!present[i]) {
          if (colbufs[i].kind != CK_NONE) {
            colbufs[i].valid.push_back(0);
            switch (colbufs[i].kind) { case CK_I8: colbufs[i].i8.push_back(0); break; case CK_I32: colbufs[i].i32.push_back(0); break; case CK_I64: colbufs[i].i64.push_back(0); break; case CK_F32: colbufs[i].f32.push_back(0.0f); break; case CK_BOOL: colbufs[i].b8.push_back(0); break; case CK_STR: colbufs[i].soff.push_back(colbufs[i].soff.back()); break; default: break; }
          } else { builders[i]->AppendNull(); }
        }
      }
      env->DeleteLocalRef(buf);
    }

    // Flush per-batch into builders (same as array[] path)
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto& b = builders[i];
      switch (colbufs[i].kind) {
        case CK_I8: { auto* nb = static_cast<arrow::Int8Builder*>(b.get()); nb->AppendValues(colbufs[i].i8.data(), static_cast<int64_t>(colbufs[i].i8.size()), colbufs[i].valid.data()); break; }
        case CK_I32: { auto* nb = static_cast<arrow::Int32Builder*>(b.get()); nb->AppendValues(colbufs[i].i32.data(), static_cast<int64_t>(colbufs[i].i32.size()), colbufs[i].valid.data()); break; }
        case CK_I64: { auto* nb = static_cast<arrow::Int64Builder*>(b.get()); nb->AppendValues(colbufs[i].i64.data(), static_cast<int64_t>(colbufs[i].i64.size()), colbufs[i].valid.data()); break; }
        case CK_F32: { auto* nb = static_cast<arrow::FloatBuilder*>(b.get()); nb->AppendValues(colbufs[i].f32.data(), static_cast<int64_t>(colbufs[i].f32.size()), colbufs[i].valid.data()); break; }
        case CK_BOOL: { auto* nb = static_cast<arrow::BooleanBuilder*>(b.get()); nb->AppendValues(colbufs[i].b8.data(), static_cast<int64_t>(colbufs[i].b8.size()), colbufs[i].valid.data()); break; }
        case CK_STR: { auto* sb = static_cast<arrow::StringBuilder*>(b.get()); int64_t rows = static_cast<int64_t>(colbufs[i].valid.size()); int64_t total_bytes = static_cast<int64_t>(colbufs[i].sdata.size()); sb->Reserve(rows); sb->ReserveData(total_bytes); for (int64_t r=0;r<rows;++r){ if (colbufs[i].valid[r]) { int32_t b0=colbufs[i].soff[r]; int32_t b1=colbufs[i].soff[r+1]; sb->Append(reinterpret_cast<const char*>(colbufs[i].sdata.data()+b0), b1-b0);} else { sb->AppendNull(); } } break; }
        case CK_LIST_I8: case CK_LIST_I32: case CK_LIST_I64: case CK_LIST_F32: case CK_LIST_BOOL: case CK_LIST_STR: {
          auto* lb = static_cast<arrow::ListBuilder*>(b.get()); int64_t rows = static_cast<int64_t>(colbufs[i].valid.size());
          switch (colbufs[i].kind) {
            case CK_LIST_I8: { auto* vb = static_cast<arrow::Int8Builder*>(lb->value_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].l_off[r], b1=colbufs[i].l_off[r+1]; if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(colbufs[i].l_i8.data()+b0, b1-b0);} break; }
            case CK_LIST_I32: { auto* vb = static_cast<arrow::Int32Builder*>(lb->value_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].l_off[r], b1=colbufs[i].l_off[r+1]; if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(colbufs[i].l_i32.data()+b0, b1-b0);} break; }
            case CK_LIST_I64: { auto* vb = static_cast<arrow::Int64Builder*>(lb->value_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].l_off[r], b1=colbufs[i].l_off[r+1]; if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(colbufs[i].l_i64.data()+b0, b1-b0);} break; }
            case CK_LIST_F32: { auto* vb = static_cast<arrow::FloatBuilder*>(lb->value_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].l_off[r], b1=colbufs[i].l_off[r+1]; if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(colbufs[i].l_f32.data()+b0, b1-b0);} break; }
            case CK_LIST_BOOL: { auto* vb = static_cast<arrow::BooleanBuilder*>(lb->value_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].l_off[r], b1=colbufs[i].l_off[r+1]; if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; } lb->Append(true); vb->AppendValues(colbufs[i].l_b8.data()+b0, b1-b0);} break; }
            case CK_LIST_STR: { auto* vb = static_cast<arrow::StringBuilder*>(lb->value_builder()); for (int64_t r=0;r<rows;++r){ int32_t e0=colbufs[i].l_off[r], e1=colbufs[i].l_off[r+1]; if (!colbufs[i].valid[r]) { lb->AppendNull(); continue; } lb->Append(true); for (int32_t e=e0; e<e1; ++e){ int32_t s0=colbufs[i].l_soff[e], s1=colbufs[i].l_soff[e+1]; vb->Append(reinterpret_cast<const char*>(colbufs[i].l_sdata.data()+s0), s1-s0);} } break; }
            default: break;
          }
          break;
        }
        case CK_MAP_STR_I64: case CK_MAP_STR_F32: case CK_MAP_STR_STR: {
          auto* mb = static_cast<arrow::MapBuilder*>(b.get()); int64_t rows = static_cast<int64_t>(colbufs[i].valid.size()); auto* kb = static_cast<arrow::StringBuilder*>(mb->key_builder());
          switch (colbufs[i].kind) {
            case CK_MAP_STR_I64: { auto* vb = static_cast<arrow::Int64Builder*>(mb->item_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].m_off[r], b1=colbufs[i].m_off[r+1]; if (!colbufs[i].valid[r]) { mb->AppendNull(); continue; } mb->Append(); for (int32_t e=b0;e<b1;++e){ int32_t k0=colbufs[i].mk_soff[e], k1=colbufs[i].mk_soff[e+1]; kb->Append(reinterpret_cast<const char*>(colbufs[i].mk_sdata.data()+k0), k1-k0);} vb->AppendValues(colbufs[i].mv_i64.data()+b0, b1-b0);} break; }
            case CK_MAP_STR_F32: { auto* vb = static_cast<arrow::FloatBuilder*>(mb->item_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].m_off[r], b1=colbufs[i].m_off[r+1]; if (!colbufs[i].valid[r]) { mb->AppendNull(); continue; } mb->Append(); for (int32_t e=b0;e<b1;++e){ int32_t k0=colbufs[i].mk_soff[e], k1=colbufs[i].mk_soff[e+1]; kb->Append(reinterpret_cast<const char*>(colbufs[i].mk_sdata.data()+k0), k1-k0);} vb->AppendValues(colbufs[i].mv_f32.data()+b0, b1-b0);} break; }
            case CK_MAP_STR_STR: { auto* vb = static_cast<arrow::StringBuilder*>(mb->item_builder()); for (int64_t r=0;r<rows;++r){ int32_t b0=colbufs[i].m_off[r], b1=colbufs[i].m_off[r+1]; if (!colbufs[i].valid[r]) { mb->AppendNull(); continue; } mb->Append(); for (int32_t e=b0;e<b1;++e){ int32_t k0=colbufs[i].mk_soff[e], k1=colbufs[i].mk_soff[e+1]; kb->Append(reinterpret_cast<const char*>(colbufs[i].mk_sdata.data()+k0), k1-k0); int32_t v0=colbufs[i].mv_soff[e], v1=colbufs[i].mv_soff[e+1]; vb->Append(reinterpret_cast<const char*>(colbufs[i].mv_sdata.data()+v0), v1-v0);} } break; }
            default: break;
          }
          break;
        }
        case CK_NONE: default: break;
      }
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays; arrays.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) { std::shared_ptr<arrow::Array> a; builders[i]->Finish(&a); arrays.push_back(std::move(a)); }
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(n), arrays);

  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*schema.get(), schema_out);
  arrow::ExportRecordBatch(*batch.get(), array_out, schema_out);
}
#endif

// Minimal stub: direct-buffer variant returns an empty batch with N rows.
extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeThriftDecoder_decodeFromDirect(
    JNIEnv* env, jclass,
    jobjectArray jbuffers, jstring, jstring,
    jlong schema_out_addr, jlong array_out_addr) {
  jsize n = env->GetArrayLength(jbuffers);
  auto schema = arrow::schema({});
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(n), arrays);
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out  = reinterpret_cast<ArrowArray*>(array_out_addr);
  arrow::ExportSchema(*schema.get(), schema_out);
  arrow::ExportRecordBatch(*batch.get(), array_out, schema_out);
}
