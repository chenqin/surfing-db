// Wrapper to build JNI library from the module; includes the existing implementation.
// TODO: Inline and remove the include once the source is fully relocated.
// Moved JNI implementation into this module to decouple from repo root
// Original source was at src/meta/thrift_decode_jni.cpp

#include <jni.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>
#include <arrow/util/bit_util.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <unordered_map>
#include <thread>
#include "meta/thrift_arrow_lite.h"

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TType;
using apache::thrift::transport::TMemoryBuffer;

namespace {

// Parser removed; using shared Arrow-only parser in meta/thrift_arrow_lite.*

static void DecodeAndAppend(TBinaryProtocol* prot, TType ttype, arrow::ArrayBuilder* bldr) {
  switch (ttype) {
    case TType::T_BOOL: { bool v; prot->readBool(v); auto* vb = static_cast<arrow::BooleanBuilder*>(bldr); vb->Append(v); break; }
    case TType::T_BYTE: { int8_t v; prot->readByte(v); auto* vb = static_cast<arrow::Int8Builder*>(bldr); vb->Append(v); break; }
    case TType::T_I16: { int16_t v; prot->readI16(v); auto* vb = static_cast<arrow::Int32Builder*>(bldr); vb->Append(static_cast<int32_t>(v)); break; }
    case TType::T_I32: { int32_t v; prot->readI32(v); auto* vb = static_cast<arrow::Int32Builder*>(bldr); vb->Append(v); break; }
    case TType::T_I64: { int64_t v; prot->readI64(v); auto* vb = static_cast<arrow::Int64Builder*>(bldr); vb->Append(v); break; }
    case TType::T_DOUBLE: { double v; prot->readDouble(v); auto* vb = static_cast<arrow::FloatBuilder*>(bldr); vb->Append(static_cast<float>(v)); break; }
    case TType::T_STRING: { std::string s; prot->readBinary(s); auto* vb = static_cast<arrow::StringBuilder*>(bldr); vb->Append(s); break; }
    default: { prot->skip(ttype); bldr->AppendNull(); }
  }
}

static std::vector<uint8_t> BuildValidityBitmap(const std::vector<uint8_t>& valid_flags, int64_t* null_count) {
  int64_t nulls = 0;
  for (uint8_t v : valid_flags) {
    if (!v) {
      ++nulls;
    }
  }
  if (null_count) {
    *null_count = nulls;
  }
  if (nulls == 0) {
    return {};
  }
  std::vector<uint8_t> bitmap(arrow::bit_util::BytesForBits(valid_flags.size()), 0);
  for (int64_t i = 0; i < static_cast<int64_t>(valid_flags.size()); ++i) {
    if (valid_flags[i]) {
      arrow::bit_util::SetBit(bitmap.data(), i);
    }
  }
  return bitmap;
}

} // namespace

// JNI decode (byte[][] path)
extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeThriftDecoder_decode(
    JNIEnv* env, jclass,
    jobjectArray jpayloads, jstring jpath, jstring jstruct,
    jlong schema_out_addr, jlong array_out_addr) {
  const char* cpath = env->GetStringUTFChars(jpath, nullptr);
  const char* cstruct = env->GetStringUTFChars(jstruct, nullptr);
  std::string thrift_path(cpath ? cpath : "");
  std::string struct_name(cstruct ? cstruct : "");
  if (cpath) env->ReleaseStringUTFChars(jpath, cpath);
  if (cstruct) env->ReleaseStringUTFChars(jstruct, cstruct);

  auto parsed = matcha::meta::ThriftArrowLiteParser::parseToArrowWithIdMap(thrift_path, struct_name);
  auto id_to_index = parsed.id_to_index;
  auto nested_id_maps = parsed.nested_id_maps;
  auto schema = parsed.schema;
  if (!schema || schema->num_fields() == 0) return;

  jsize n = env->GetArrayLength(jpayloads);
  enum ColKind { CK_NONE, CK_I8, CK_I32, CK_I64, CK_F32, CK_BOOL, CK_STR, CK_LIST_I8, CK_LIST_I32, CK_LIST_I64, CK_LIST_F32, CK_LIST_BOOL, CK_LIST_STR, CK_MAP_STR_I64, CK_MAP_STR_F32, CK_MAP_STR_STR, CK_MAP_STR_LIST_I64, CK_MAP_STR_LIST_STR, CK_MAP_STR_LIST_STRUCT };
  struct ColBuffers {
    ColKind kind{CK_NONE}; std::vector<int8_t> i8; std::vector<int32_t> i32; std::vector<int64_t> i64; std::vector<float> f32; std::vector<uint8_t> b8; std::vector<uint8_t> valid;
    std::vector<uint8_t> sdata; std::vector<int32_t> soff;
    std::vector<int32_t> l_off; std::vector<int8_t> l_i8; std::vector<int32_t> l_i32; std::vector<int64_t> l_i64; std::vector<float> l_f32; std::vector<uint8_t> l_b8; std::vector<uint8_t> l_sdata; std::vector<int32_t> l_soff;
    std::vector<int32_t> m_off; std::vector<uint8_t> mk_sdata; std::vector<int32_t> mk_soff; std::vector<int64_t> mv_i64; std::vector<float> mv_f32; std::vector<uint8_t> mv_sdata; std::vector<int32_t> mv_soff;
  };
  struct FieldContext {
    ColKind kind{CK_NONE};
    arrow::Type::type type_id{arrow::Type::NA};
    const arrow::DataType* data_type{nullptr};
    const arrow::ListType* list_type{nullptr};
    const arrow::MapType* map_type{nullptr};
    const arrow::ListType* map_list_type{nullptr};
    const arrow::StructType* map_struct_type{nullptr};
    bool list_of_struct{false};
    const std::unordered_map<int, int>* nested_field_map{nullptr};
    arrow::ArrayBuilder* builder{nullptr};
    arrow::ListBuilder* list_builder{nullptr};
    arrow::StructBuilder* list_struct_builder{nullptr};
    arrow::MapBuilder* map_builder{nullptr};
  };
  std::vector<ColBuffers> colbufs(schema->num_fields());
  std::vector<FieldContext> field_ctx(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    const auto& field = schema->field(i);
    auto type = field->type();
    auto tid = type->id(); ColKind k = CK_NONE;
    switch (tid) {
      case arrow::Type::INT8: k = CK_I8; break; case arrow::Type::INT16: k = CK_I32; break; case arrow::Type::INT32: k = CK_I32; break; case arrow::Type::INT64: k = CK_I64; break; case arrow::Type::FLOAT: k = CK_F32; break; case arrow::Type::BOOL: k = CK_BOOL; break; case arrow::Type::STRING: k = CK_STR; break;
      case arrow::Type::LIST: { auto* lt = static_cast<const arrow::ListType*>(type.get()); switch (lt->value_type()->id()) {
        case arrow::Type::INT8: k = CK_LIST_I8; break; case arrow::Type::INT16: case arrow::Type::INT32: k = CK_LIST_I32; break; case arrow::Type::INT64: k = CK_LIST_I64; break; case arrow::Type::FLOAT: k = CK_LIST_F32; break; case arrow::Type::BOOL: k = CK_LIST_BOOL; break; case arrow::Type::STRING: k = CK_LIST_STR; break; default: k = CK_NONE; break; } break; }
      case arrow::Type::MAP: { auto* mt = static_cast<const arrow::MapType*>(type.get()); if (mt->key_type()->id() == arrow::Type::STRING) { auto value_id = mt->item_type()->id(); switch (value_id) { case arrow::Type::INT64: k = CK_MAP_STR_I64; break; case arrow::Type::FLOAT: k = CK_MAP_STR_F32; break; case arrow::Type::STRING: k = CK_MAP_STR_STR; break; case arrow::Type::LIST: { auto* lt = static_cast<const arrow::ListType*>(mt->item_type().get()); switch (lt->value_type()->id()) { case arrow::Type::INT64: k = CK_MAP_STR_LIST_I64; break; case arrow::Type::STRING: k = CK_MAP_STR_LIST_STR; break; case arrow::Type::STRUCT: k = CK_MAP_STR_LIST_STRUCT; break; default: k = CK_NONE; break; } break; } default: k = CK_NONE; break; } } break; }
      default: k = CK_NONE; break;
    }
    field_ctx[i].kind = k;
    field_ctx[i].type_id = tid;
    field_ctx[i].data_type = type.get();
    if (tid == arrow::Type::LIST) {
      field_ctx[i].list_type = static_cast<const arrow::ListType*>(type.get());
      field_ctx[i].list_of_struct = field_ctx[i].list_type && field_ctx[i].list_type->value_type()->id() == arrow::Type::STRUCT;
    } else if (tid == arrow::Type::MAP) {
      field_ctx[i].map_type = static_cast<const arrow::MapType*>(type.get());
    }
    if (static_cast<size_t>(i) < nested_id_maps.size()) {
      field_ctx[i].nested_field_map = &nested_id_maps[i];
    }
    if (tid == arrow::Type::MAP && field_ctx[i].map_type && field_ctx[i].map_type->item_type()->id() == arrow::Type::LIST) {
      field_ctx[i].map_list_type = static_cast<const arrow::ListType*>(field_ctx[i].map_type->item_type().get());
      if (field_ctx[i].map_list_type && field_ctx[i].map_list_type->value_type()->id() == arrow::Type::STRUCT) {
        field_ctx[i].map_struct_type = static_cast<const arrow::StructType*>(field_ctx[i].map_list_type->value_type().get());
      }
    }
    colbufs[i].kind = k;
    if (k != CK_NONE) {
      colbufs[i].valid.reserve(n);
      switch (k) {
        case CK_I8: colbufs[i].i8.reserve(n); break; case CK_I32: colbufs[i].i32.reserve(n); break; case CK_I64: colbufs[i].i64.reserve(n); break; case CK_F32: colbufs[i].f32.reserve(n); break; case CK_BOOL: colbufs[i].b8.reserve(n); break;
        case CK_STR: colbufs[i].sdata.reserve(n * 8); colbufs[i].soff.reserve(n + 1); colbufs[i].soff.push_back(0); break;
        case CK_LIST_I8: case CK_LIST_I32: case CK_LIST_I64: case CK_LIST_F32: case CK_LIST_BOOL: case CK_LIST_STR: colbufs[i].l_off.reserve(n + 1); colbufs[i].l_off.push_back(0); break;
        case CK_MAP_STR_I64: case CK_MAP_STR_F32: case CK_MAP_STR_STR: colbufs[i].m_off.reserve(n + 1); colbufs[i].m_off.push_back(0); colbufs[i].mk_soff.reserve(n + 1); colbufs[i].mk_soff.push_back(0); if (k == CK_MAP_STR_STR) { colbufs[i].mv_soff.reserve(n + 1); colbufs[i].mv_soff.push_back(0); } break;
        default: break;
      }
    }
  }

  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders; builders.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) { std::unique_ptr<arrow::ArrayBuilder> b; arrow::MakeBuilder(arrow::default_memory_pool(), schema->field(i)->type(), &b); if (b) { b->Reserve(static_cast<int64_t>(n)); field_ctx[i].builder = b.get(); if (field_ctx[i].type_id == arrow::Type::LIST) { field_ctx[i].list_builder = static_cast<arrow::ListBuilder*>(b.get()); if (field_ctx[i].list_builder && field_ctx[i].list_of_struct) { field_ctx[i].list_struct_builder = static_cast<arrow::StructBuilder*>(field_ctx[i].list_builder->value_builder()); } } else if (field_ctx[i].type_id == arrow::Type::MAP) { field_ctx[i].map_builder = static_cast<arrow::MapBuilder*>(b.get()); } } builders.push_back(std::move(b)); }

  const int kBatch = 4000; auto mem = std::make_shared<TMemoryBuffer>(); TBinaryProtocol prot(mem); std::vector<uint8_t> present(static_cast<size_t>(schema->num_fields()), 0);
  for (jsize start = 0; start < n; start += kBatch) {
    jsize end = std::min<jsize>(n, start + kBatch);
    for (int i = 0; i < schema->num_fields(); ++i) if (colbufs[i].kind != CK_NONE) { colbufs[i].valid.clear(); switch (colbufs[i].kind) { case CK_I8: colbufs[i].i8.clear(); break; case CK_I32: colbufs[i].i32.clear(); break; case CK_I64: colbufs[i].i64.clear(); break; case CK_F32: colbufs[i].f32.clear(); break; case CK_BOOL: colbufs[i].b8.clear(); break; case CK_STR: colbufs[i].sdata.clear(); colbufs[i].soff.clear(); colbufs[i].soff.push_back(0); break; case CK_LIST_STR: colbufs[i].l_off.clear(); colbufs[i].l_off.push_back(0); colbufs[i].l_sdata.clear(); colbufs[i].l_soff.clear(); colbufs[i].l_soff.push_back(0); break; default: break; } }
    for (jsize r = start; r < end; ++r) {
      jbyteArray arr = (jbyteArray) env->GetObjectArrayElement(jpayloads, r); if (!arr) continue; jsize len = env->GetArrayLength(arr);
      jboolean is_copy = JNI_FALSE; jbyte* bytes = (jbyte*) env->GetPrimitiveArrayCritical(arr, &is_copy);
      mem->resetBuffer(reinterpret_cast<uint8_t*>(bytes), static_cast<uint32_t>(len)); std::memset(present.data(), 0, present.size()); std::string sname; prot.readStructBegin(sname);
      while (true) {
        std::string fname; TType ftype; int16_t fid; prot.readFieldBegin(fname, ftype, fid); if (ftype == TType::T_STOP) break; auto it = id_to_index.find(fid); if (it == id_to_index.end()) { prot.skip(ftype); prot.readFieldEnd(); continue; }
        int idx = it->second;
        auto& ctx = field_ctx[idx];
        auto& cb = colbufs[idx];
        auto colk = cb.kind;
        auto* builder = ctx.builder;
        switch (ctx.type_id) {
          case arrow::Type::NA: {
            prot.skip(ftype);
            if (builder) builder->AppendNull();
            present[idx] = 1;
            break;
          }
          case arrow::Type::BOOL: {
            if (colk == CK_BOOL && ftype == TType::T_BOOL) {
              bool v;
              prot.readBool(v);
              cb.b8.push_back(v ? 1 : 0);
              cb.valid.push_back(1);
              present[idx] = 1;
            } else if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
            }
            break;
          }
          case arrow::Type::INT8: {
            if (colk == CK_I8 && ftype == TType::T_BYTE) {
              int8_t v;
              prot.readByte(v);
              cb.i8.push_back(v);
              cb.valid.push_back(1);
              present[idx] = 1;
            } else if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
            }
            break;
          }
          case arrow::Type::INT16:
          case arrow::Type::INT32: {
            if (colk == CK_I32 && (ftype == TType::T_I16 || ftype == TType::T_I32)) {
              int32_t outv = 0;
              if (ftype == TType::T_I16) {
                int16_t t;
                prot.readI16(t);
                outv = static_cast<int32_t>(t);
              } else {
                int32_t t;
                prot.readI32(t);
                outv = t;
              }
              cb.i32.push_back(outv);
              cb.valid.push_back(1);
              present[idx] = 1;
            } else if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
            }
            break;
          }
          case arrow::Type::INT64: {
            if (colk == CK_I64 && ftype == TType::T_I64) {
              int64_t v;
              prot.readI64(v);
              cb.i64.push_back(v);
              cb.valid.push_back(1);
              present[idx] = 1;
            } else if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
            }
            break;
          }
          case arrow::Type::FLOAT:
          case arrow::Type::DOUBLE: {
            if (colk == CK_F32 && ftype == TType::T_DOUBLE) {
              double dv;
              prot.readDouble(dv);
              cb.f32.push_back(static_cast<float>(dv));
              cb.valid.push_back(1);
              present[idx] = 1;
            } else if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
            }
            break;
          }
          case arrow::Type::STRING: {
            if (colk == CK_STR && ftype == TType::T_STRING) {
              int32_t l = 0;
              prot.readI32(l);
              if (l > 0) {
                uint32_t want = static_cast<uint32_t>(l);
                const uint8_t* p = mem->borrow(nullptr, &want);
                if (p && want >= static_cast<uint32_t>(l)) {
                  cb.sdata.insert(cb.sdata.end(), p, p + l);
                  cb.soff.push_back(cb.soff.back() + l);
                  mem->consume(static_cast<uint32_t>(l));
                } else {
                  std::vector<uint8_t> tmp(l);
                  uint32_t got = 0;
                  while (got < static_cast<uint32_t>(l)) {
                    uint32_t r2 = mem->read(tmp.data() + got, static_cast<uint32_t>(l) - got);
                    if (r2 == 0) break;
                    got += r2;
                  }
                  cb.sdata.insert(cb.sdata.end(), tmp.begin(), tmp.end());
                  cb.soff.push_back(cb.soff.back() + l);
                }
              } else {
                cb.soff.push_back(cb.soff.back());
              }
              cb.valid.push_back(1);
              present[idx] = 1;
            } else if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
            }
            break;
          }
          case arrow::Type::LIST: {
            if (ctx.list_of_struct && ftype == TType::T_LIST) {
              apache::thrift::protocol::TType et;
              uint32_t sz;
              prot.readListBegin(et, sz);
              if (et == TType::T_STRUCT && ctx.list_builder && ctx.list_struct_builder) {
                auto* lb = ctx.list_builder;
                auto* sb = ctx.list_struct_builder;
                const std::unordered_map<int, int>* nid2pos_ptr = ctx.nested_field_map;
                lb->Append(true);
                for (uint32_t j = 0; j < sz; ++j) {
                  std::string s2;
                  prot.readStructBegin(s2);
                  sb->Append(true);
                  std::vector<uint8_t> child_present(static_cast<size_t>(sb->num_fields()), 0);
                  while (true) {
                    std::string fn;
                    TType ft2;
                    int16_t fid2;
                    prot.readFieldBegin(fn, ft2, fid2);
                    if (ft2 == TType::T_STOP) break;
                    int cpos = -1;
                    if (nid2pos_ptr) {
                      auto itp = nid2pos_ptr->find(fid2);
                      if (itp != nid2pos_ptr->end()) cpos = itp->second;
                    }
                    if (cpos >= 0) {
                      auto* cbb = sb->field_builder(cpos);
                      switch (sb->type()->field(cpos)->type()->id()) {
                        case arrow::Type::INT64:
                        case arrow::Type::INT32:
                        case arrow::Type::INT8:
                        case arrow::Type::FLOAT:
                        case arrow::Type::BOOL: {
                          DecodeAndAppend(&prot, ft2, cbb);
                          child_present[cpos] = 1;
                          break;
                        }
                        case arrow::Type::STRING: {
                          if (ft2 == TType::T_STRING) {
                            std::string s;
                            prot.readBinary(s);
                            static_cast<arrow::StringBuilder*>(cbb)->Append(s);
                            child_present[cpos] = 1;
                          } else {
                            prot.skip(ft2);
                          }
                          break;
                        }
                        default:
                          prot.skip(ft2);
                          break;
                      }
                    } else {
                      prot.skip(ft2);
                    }
                    prot.readFieldEnd();
                  }
                  prot.readStructEnd();
                  for (int cc = 0; cc < sb->num_fields(); ++cc) {
                    if (!child_present[cc]) sb->field_builder(cc)->AppendNull();
                  }
                }
                prot.readListEnd();
                present[idx] = 1;
              } else {
                prot.skip(et);
                present[idx] = 1;
              }
            } else {
              if (colk == CK_LIST_I8 || colk == CK_LIST_I32 || colk == CK_LIST_I64 || colk == CK_LIST_F32 || colk == CK_LIST_BOOL || colk == CK_LIST_STR) {
                if (ftype == TType::T_LIST) {
                  apache::thrift::protocol::TType et;
                  uint32_t sz;
                  prot.readListBegin(et, sz);
                  switch (colk) {
                    case CK_LIST_I8: {
                      if (et == TType::T_BYTE) {
                        uint32_t total = sz;
                        uint32_t want = total;
                        const uint8_t* p = mem->borrow(nullptr, &want);
                        if (p && want >= total) {
                          cb.l_i8.insert(cb.l_i8.end(), p, p + total);
                          cb.valid.push_back(1);
                          cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                          mem->consume(total);
                        } else {
                          cb.l_i8.reserve(cb.l_i8.size() + sz);
                          for (uint32_t j = 0; j < sz; ++j) {
                            int8_t v;
                            prot.readByte(v);
                            cb.l_i8.push_back(v);
                          }
                          cb.valid.push_back(1);
                          cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                        }
                      } else {
                        prot.skip(et);
                        cb.valid.push_back(0);
                        cb.l_off.push_back(cb.l_off.back());
                      }
                      break;
                    }
                    case CK_LIST_I32: {
                      if (et == TType::T_I32) {
                        uint32_t total = sz * 4U;
                        uint32_t want = total;
                        const uint8_t* p = mem->borrow(nullptr, &want);
                        if (p && want >= total) {
                          cb.l_i32.reserve(cb.l_i32.size() + sz);
                          for (uint32_t j = 0; j < sz; ++j) {
                            uint32_t be = *reinterpret_cast<const uint32_t*>(p + j * 4);
                            uint32_t le = __builtin_bswap32(be);
                            cb.l_i32.push_back(static_cast<int32_t>(le));
                          }
                          cb.valid.push_back(1);
                          cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                          mem->consume(total);
                        } else {
                          cb.l_i32.reserve(cb.l_i32.size() + sz);
                          for (uint32_t j = 0; j < sz; ++j) {
                            int32_t v;
                            prot.readI32(v);
                            cb.l_i32.push_back(v);
                          }
                          cb.valid.push_back(1);
                          cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                        }
                      } else if (et == TType::T_I16) {
                        cb.l_i32.reserve(cb.l_i32.size() + sz);
                        for (uint32_t j = 0; j < sz; ++j) {
                          int16_t v;
                          prot.readI16(v);
                          cb.l_i32.push_back(static_cast<int32_t>(v));
                        }
                        cb.valid.push_back(1);
                        cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                      } else {
                        prot.skip(et);
                        cb.valid.push_back(0);
                        cb.l_off.push_back(cb.l_off.back());
                      }
                      break;
                    }
                    case CK_LIST_I64: {
                      if (et == TType::T_I64) {
                        uint32_t total = sz * 8U;
                        uint32_t want = total;
                        const uint8_t* p = mem->borrow(nullptr, &want);
                        if (p && want >= total) {
                          cb.l_i64.reserve(cb.l_i64.size() + sz);
                          for (uint32_t j = 0; j < sz; ++j) {
                            uint64_t be = *reinterpret_cast<const uint64_t*>(p + j * 8);
                            uint64_t le = __builtin_bswap64(be);
                            cb.l_i64.push_back(static_cast<int64_t>(le));
                          }
                          cb.valid.push_back(1);
                          cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                          mem->consume(total);
                        } else {
                          cb.l_i64.reserve(cb.l_i64.size() + sz);
                          for (uint32_t j = 0; j < sz; ++j) {
                            int64_t v;
                            prot.readI64(v);
                            cb.l_i64.push_back(v);
                          }
                          cb.valid.push_back(1);
                          cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                        }
                      } else {
                        prot.skip(et);
                        cb.valid.push_back(0);
                        cb.l_off.push_back(cb.l_off.back());
                      }
                      break;
                    }
                    case CK_LIST_F32: {
                      if (et == TType::T_DOUBLE) {
                        cb.l_f32.reserve(cb.l_f32.size() + sz);
                        for (uint32_t j = 0; j < sz; ++j) {
                          double dv;
                          prot.readDouble(dv);
                          cb.l_f32.push_back(static_cast<float>(dv));
                        }
                        cb.valid.push_back(1);
                        cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                      } else {
                        prot.skip(et);
                        cb.valid.push_back(0);
                        cb.l_off.push_back(cb.l_off.back());
                      }
                      break;
                    }
                    case CK_LIST_BOOL: {
                      if (et == TType::T_BOOL) {
                        cb.l_b8.reserve(cb.l_b8.size() + sz);
                        for (uint32_t j = 0; j < sz; ++j) {
                          bool v;
                          prot.readBool(v);
                          cb.l_b8.push_back(v ? 1 : 0);
                        }
                        cb.valid.push_back(1);
                        cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                      } else {
                        prot.skip(et);
                        cb.valid.push_back(0);
                        cb.l_off.push_back(cb.l_off.back());
                      }
                      break;
                    }
                    case CK_LIST_STR: {
                      if (et == TType::T_STRING) {
                        cb.valid.push_back(1);
                        cb.l_off.push_back(cb.l_off.back() + static_cast<int32_t>(sz));
                        for (uint32_t j = 0; j < sz; ++j) {
                          int32_t slen = 0;
                          prot.readI32(slen);
                          if (slen > 0) {
                            uint32_t want = static_cast<uint32_t>(slen);
                            const uint8_t* p = mem->borrow(nullptr, &want);
                            if (p && want >= static_cast<uint32_t>(slen)) {
                              cb.l_sdata.insert(cb.l_sdata.end(), p, p + slen);
                              cb.l_soff.push_back(cb.l_soff.back() + slen);
                              mem->consume(static_cast<uint32_t>(slen));
                            } else {
                              std::vector<uint8_t> tmp(slen);
                              uint32_t got = 0;
                              while (got < static_cast<uint32_t>(slen)) {
                                uint32_t rr = mem->read(tmp.data() + got, static_cast<uint32_t>(slen) - got);
                                if (rr == 0) break;
                                got += rr;
                              }
                              cb.l_sdata.insert(cb.l_sdata.end(), tmp.begin(), tmp.end());
                              cb.l_soff.push_back(cb.l_soff.back() + slen);
                            }
                          } else {
                            cb.l_soff.push_back(cb.l_soff.back());
                          }
                        }
                      } else {
                        prot.skip(et);
                        cb.valid.push_back(0);
                        cb.l_off.push_back(cb.l_off.back());
                      }
                      break;
                    }
                    default:
                      break;
                  }
                  prot.readListEnd();
                  present[idx] = 1;
                } else {
                  prot.skip(ftype);
                  present[idx] = 1;
                }
              } else {
                prot.skip(ftype);
                present[idx] = 1;
              }
            }
            break;
          }
          case arrow::Type::MAP: {
            if (colk == CK_MAP_STR_I64 || colk == CK_MAP_STR_F32 || colk == CK_MAP_STR_STR) {
              if (ftype == TType::T_MAP) {
                apache::thrift::protocol::TType kt, vt;
                uint32_t sz;
                prot.readMapBegin(kt, vt, sz);
                if (kt == TType::T_STRING) {
                  for (uint32_t j = 0; j < sz; ++j) {
                    int32_t klen = 0;
                    prot.readI32(klen);
                    if (klen > 0) {
                      uint32_t want = static_cast<uint32_t>(klen);
                      const uint8_t* p = mem->borrow(nullptr, &want);
                      if (p && want >= static_cast<uint32_t>(klen)) {
                        cb.mk_sdata.insert(cb.mk_sdata.end(), p, p + klen);
                        cb.mk_soff.push_back(cb.mk_soff.back() + klen);
                        mem->consume(static_cast<uint32_t>(klen));
                      } else {
                        std::vector<uint8_t> tmp(klen);
                        uint32_t got = 0;
                        while (got < static_cast<uint32_t>(klen)) {
                          uint32_t rr = mem->read(tmp.data() + got, static_cast<uint32_t>(klen) - got);
                          if (rr == 0) break;
                          got += rr;
                        }
                        cb.mk_sdata.insert(cb.mk_sdata.end(), tmp.begin(), tmp.end());
                        cb.mk_soff.push_back(cb.mk_soff.back() + klen);
                      }
                    } else {
                      cb.mk_soff.push_back(cb.mk_soff.back());
                    }
                    switch (colk) {
                      case CK_MAP_STR_I64: {
                        int64_t v;
                        prot.readI64(v);
                        cb.mv_i64.push_back(v);
                        break;
                      }
                      case CK_MAP_STR_F32: {
                        double dv;
                        prot.readDouble(dv);
                        cb.mv_f32.push_back(static_cast<float>(dv));
                        break;
                      }
                      case CK_MAP_STR_STR: {
                        int32_t vlen = 0;
                        prot.readI32(vlen);
                        if (vlen > 0) {
                          uint32_t want = static_cast<uint32_t>(vlen);
                          const uint8_t* p2 = mem->borrow(nullptr, &want);
                          if (p2 && want >= static_cast<uint32_t>(vlen)) {
                            cb.mv_sdata.insert(cb.mv_sdata.end(), p2, p2 + vlen);
                            cb.mv_soff.push_back(cb.mv_soff.back() + vlen);
                            mem->consume(static_cast<uint32_t>(vlen));
                          } else {
                            std::vector<uint8_t> tmp2(vlen);
                            uint32_t got2 = 0;
                            while (got2 < static_cast<uint32_t>(vlen)) {
                              uint32_t rr2 = mem->read(tmp2.data() + got2, static_cast<uint32_t>(vlen) - got2);
                              if (rr2 == 0) break;
                              got2 += rr2;
                            }
                            cb.mv_sdata.insert(cb.mv_sdata.end(), tmp2.begin(), tmp2.end());
                            cb.mv_soff.push_back(cb.mv_soff.back() + vlen);
                          }
                        } else {
                          cb.mv_soff.push_back(cb.mv_soff.back());
                        }
                        break;
                      }
                      default:
                        break;
                    }
                  }
                  cb.valid.push_back(1);
                  cb.m_off.push_back(cb.m_off.back() + static_cast<int32_t>(sz));
                } else {
                  prot.skip(vt);
                  cb.valid.push_back(0);
                  cb.m_off.push_back(cb.m_off.back());
                }
                prot.readMapEnd();
                present[idx] = 1;
              } else {
                prot.skip(ftype);
                present[idx] = 1;
              }
            } else if ((colk == CK_MAP_STR_LIST_I64 || colk == CK_MAP_STR_LIST_STR || colk == CK_MAP_STR_LIST_STRUCT) && ctx.map_builder) {
              if (ftype == TType::T_MAP) {
                apache::thrift::protocol::TType kt, vt;
                uint32_t sz;
                prot.readMapBegin(kt, vt, sz);
                if (kt == TType::T_STRING && vt == TType::T_LIST) {
                  auto* mb = ctx.map_builder;
                  auto* key_builder = static_cast<arrow::StringBuilder*>(mb->key_builder());
                  auto* list_builder = static_cast<arrow::ListBuilder*>(mb->item_builder());
                  auto appendString = [&](arrow::StringBuilder* sb, int32_t len) {
                    if (len <= 0) {
                      sb->Append("");
                      return;
                    }
                    uint32_t want = static_cast<uint32_t>(len);
                    const uint8_t* p = mem->borrow(nullptr, &want);
                    if (p && want >= static_cast<uint32_t>(len)) {
                      sb->Append(reinterpret_cast<const char*>(p), len);
                      mem->consume(static_cast<uint32_t>(len));
                    } else {
                      std::vector<uint8_t> tmp(len);
                      uint32_t got = 0;
                      while (got < static_cast<uint32_t>(len)) {
                        uint32_t rr = mem->read(tmp.data() + got, static_cast<uint32_t>(len) - got);
                        if (rr == 0) break;
                        got += rr;
                      }
                      sb->Append(reinterpret_cast<const char*>(tmp.data()), len);
                    }
                  };
                  for (uint32_t j = 0; j < sz; ++j) {
                    mb->Append();
                    int32_t klen = 0;
                    prot.readI32(klen);
                    appendString(key_builder, klen);
                    apache::thrift::protocol::TType et;
                    uint32_t lsz;
                    prot.readListBegin(et, lsz);
                    if (colk == CK_MAP_STR_LIST_I64 && et == TType::T_I64) {
                      list_builder->Append(true);
                      auto* vb = static_cast<arrow::Int64Builder*>(list_builder->value_builder());
                      for (uint32_t e = 0; e < lsz; ++e) {
                        int64_t v;
                        prot.readI64(v);
                        vb->Append(v);
                      }
                      prot.readListEnd();
                    } else if (colk == CK_MAP_STR_LIST_STR && et == TType::T_STRING) {
                      list_builder->Append(true);
                      auto* vb = static_cast<arrow::StringBuilder*>(list_builder->value_builder());
                      for (uint32_t e = 0; e < lsz; ++e) {
                        int32_t slen = 0;
                        prot.readI32(slen);
                        appendString(vb, slen);
                      }
                      prot.readListEnd();
                    } else if (colk == CK_MAP_STR_LIST_STRUCT && et == TType::T_STRUCT) {
                      list_builder->Append(true);
                      auto* sbuilder = static_cast<arrow::StructBuilder*>(list_builder->value_builder());
                      const std::unordered_map<int, int>* nid2pos_ptr = ctx.nested_field_map;
                      for (uint32_t e = 0; e < lsz; ++e) {
                        std::string s3;
                        prot.readStructBegin(s3);
                        sbuilder->Append(true);
                        std::vector<uint8_t> child_present(static_cast<size_t>(sbuilder->num_fields()), 0);
                        while (true) {
                          std::string fn;
                          TType ft2;
                          int16_t fid2;
                          prot.readFieldBegin(fn, ft2, fid2);
                          if (ft2 == TType::T_STOP) break;
                          int cpos = -1;
                          if (nid2pos_ptr) {
                            auto itp = nid2pos_ptr->find(fid2);
                            if (itp != nid2pos_ptr->end()) cpos = itp->second;
                          }
                          if (cpos >= 0) {
                            auto* cbb = sbuilder->field_builder(cpos);
                            switch (sbuilder->type()->field(cpos)->type()->id()) {
                              case arrow::Type::INT64:
                              case arrow::Type::INT32:
                              case arrow::Type::INT8:
                              case arrow::Type::FLOAT:
                              case arrow::Type::BOOL: {
                                DecodeAndAppend(&prot, ft2, cbb);
                                child_present[cpos] = 1;
                                break;
                              }
                              case arrow::Type::STRING: {
                                if (ft2 == TType::T_STRING) {
                                  std::string s;
                                  prot.readBinary(s);
                                  static_cast<arrow::StringBuilder*>(cbb)->Append(s);
                                  child_present[cpos] = 1;
                                } else {
                                  prot.skip(ft2);
                                }
                                break;
                              }
                              default:
                                prot.skip(ft2);
                                break;
                            }
                          } else {
                            prot.skip(ft2);
                          }
                          prot.readFieldEnd();
                        }
                        prot.readStructEnd();
                        for (int cc = 0; cc < sbuilder->num_fields(); ++cc) {
                          if (!child_present[cc]) sbuilder->field_builder(cc)->AppendNull();
                        }
                      }
                      prot.readListEnd();
                    } else {
                      // Unexpected element type; skip remaining elements
                      for (uint32_t e = 0; e < lsz; ++e) {
                        prot.skip(et);
                      }
                      prot.readListEnd();
                      list_builder->AppendNull();
                    }
                  }
                } else {
                  prot.skip(vt);
                }
                prot.readMapEnd();
                present[idx] = 1;
              } else {
                prot.skip(ftype);
                present[idx] = 1;
              }
            } else {
              if (builder) {
                DecodeAndAppend(&prot, ftype, builder);
                present[idx] = 1;
              } else {
                prot.skip(ftype);
                present[idx] = 1;
              }
            }
            break;
          }
          default: {
            if (builder) {
              DecodeAndAppend(&prot, ftype, builder);
              present[idx] = 1;
            } else {
              prot.skip(ftype);
              present[idx] = 1;
            }
            break;
          }
        }
        prot.readFieldEnd();
      }
      prot.readStructEnd();
      for (int i = 0; i < schema->num_fields(); ++i) if (!present[i]) { if (colbufs[i].kind != CK_NONE) { colbufs[i].valid.push_back(0); switch (colbufs[i].kind) { case CK_I8: colbufs[i].i8.push_back(0); break; case CK_I32: colbufs[i].i32.push_back(0); break; case CK_I64: colbufs[i].i64.push_back(0); break; case CK_F32: colbufs[i].f32.push_back(0.0f); break; case CK_BOOL: colbufs[i].b8.push_back(0); break; case CK_STR: { colbufs[i].soff.push_back(colbufs[i].soff.back()); break; } default: break; } } else if (field_ctx[i].builder) { field_ctx[i].builder->AppendNull(); } }
env->ReleasePrimitiveArrayCritical(arr, bytes, JNI_ABORT); env->DeleteLocalRef(arr);
    }
  }

  for (int i = 0; i < schema->num_fields(); ++i) {
    auto& builder_ptr = builders[i];
    if (!builder_ptr) {
      continue;
    }
    auto& cb = colbufs[i];
    switch (cb.kind) {
      case CK_I8: {
        auto* nb = static_cast<arrow::Int8Builder*>(builder_ptr.get());
        int64_t null_count = 0;
        auto bitmap = BuildValidityBitmap(cb.valid, &null_count);
        nb->AppendValues(cb.i8.data(), static_cast<int64_t>(cb.i8.size()), bitmap.empty() ? nullptr : bitmap.data());
        break;
      }
      case CK_I32: {
        auto* nb = static_cast<arrow::Int32Builder*>(builder_ptr.get());
        int64_t null_count = 0;
        auto bitmap = BuildValidityBitmap(cb.valid, &null_count);
        nb->AppendValues(cb.i32.data(), static_cast<int64_t>(cb.i32.size()), bitmap.empty() ? nullptr : bitmap.data());
        break;
      }
      case CK_I64: {
        auto* nb = static_cast<arrow::Int64Builder*>(builder_ptr.get());
        int64_t null_count = 0;
        auto bitmap = BuildValidityBitmap(cb.valid, &null_count);
        nb->AppendValues(cb.i64.data(), static_cast<int64_t>(cb.i64.size()), bitmap.empty() ? nullptr : bitmap.data());
        break;
      }
      case CK_F32: {
        auto* nb = static_cast<arrow::FloatBuilder*>(builder_ptr.get());
        int64_t null_count = 0;
        auto bitmap = BuildValidityBitmap(cb.valid, &null_count);
        nb->AppendValues(cb.f32.data(), static_cast<int64_t>(cb.f32.size()), bitmap.empty() ? nullptr : bitmap.data());
        break;
      }
      case CK_BOOL: {
        auto* nb = static_cast<arrow::BooleanBuilder*>(builder_ptr.get());
        int64_t null_count = 0;
        auto bitmap = BuildValidityBitmap(cb.valid, &null_count);
        nb->AppendValues(cb.b8.data(), static_cast<int64_t>(cb.b8.size()), bitmap.empty() ? nullptr : bitmap.data());
        break;
      }
      case CK_STR: {
        auto* sb = static_cast<arrow::StringBuilder*>(builder_ptr.get());
        int64_t rows = static_cast<int64_t>(cb.valid.size());
        sb->Reserve(rows);
        sb->ReserveData(static_cast<int64_t>(cb.sdata.size()));
        for (int64_t r = 0; r < rows; ++r) {
          if (cb.valid[r]) {
            int32_t start_offset = cb.soff[r];
            int32_t end_offset = cb.soff[r + 1];
            sb->Append(reinterpret_cast<const char*>(cb.sdata.data() + start_offset), end_offset - start_offset);
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
        auto* lb = static_cast<arrow::ListBuilder*>(builder_ptr.get());
        int64_t rows = static_cast<int64_t>(cb.valid.size());
        switch (cb.kind) {
          case CK_LIST_I8: {
            auto* vb = static_cast<arrow::Int8Builder*>(lb->value_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.l_off[r];
              int32_t end = cb.l_off[r + 1];
              if (!cb.valid[r]) {
                lb->AppendNull();
              } else {
                lb->Append(true);
                vb->AppendValues(cb.l_i8.data() + begin, end - begin);
              }
            }
            break;
          }
          case CK_LIST_I32: {
            auto* vb = static_cast<arrow::Int32Builder*>(lb->value_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.l_off[r];
              int32_t end = cb.l_off[r + 1];
              if (!cb.valid[r]) {
                lb->AppendNull();
              } else {
                lb->Append(true);
                vb->AppendValues(cb.l_i32.data() + begin, end - begin);
              }
            }
            break;
          }
          case CK_LIST_I64: {
            auto* vb = static_cast<arrow::Int64Builder*>(lb->value_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.l_off[r];
              int32_t end = cb.l_off[r + 1];
              if (!cb.valid[r]) {
                lb->AppendNull();
              } else {
                lb->Append(true);
                vb->AppendValues(cb.l_i64.data() + begin, end - begin);
              }
            }
            break;
          }
          case CK_LIST_F32: {
            auto* vb = static_cast<arrow::FloatBuilder*>(lb->value_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.l_off[r];
              int32_t end = cb.l_off[r + 1];
              if (!cb.valid[r]) {
                lb->AppendNull();
              } else {
                lb->Append(true);
                vb->AppendValues(cb.l_f32.data() + begin, end - begin);
              }
            }
            break;
          }
          case CK_LIST_BOOL: {
            auto* vb = static_cast<arrow::BooleanBuilder*>(lb->value_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.l_off[r];
              int32_t end = cb.l_off[r + 1];
              if (!cb.valid[r]) {
                lb->AppendNull();
              } else {
                lb->Append(true);
                vb->AppendValues(cb.l_b8.data() + begin, end - begin);
              }
            }
            break;
          }
          case CK_LIST_STR: {
            auto* vb = static_cast<arrow::StringBuilder*>(lb->value_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.l_off[r];
              int32_t end = cb.l_off[r + 1];
              if (!cb.valid[r]) {
                lb->AppendNull();
                continue;
              }
              lb->Append(true);
              for (int32_t e = begin; e < end; ++e) {
                int32_t start_offset = cb.l_soff[e];
                int32_t end_offset = cb.l_soff[e + 1];
                vb->Append(reinterpret_cast<const char*>(cb.l_sdata.data() + start_offset), end_offset - start_offset);
              }
            }
            break;
          }
          default:
            break;
        }
        break;
      }
      case CK_MAP_STR_I64:
      case CK_MAP_STR_F32:
      case CK_MAP_STR_STR: {
        auto* mb = static_cast<arrow::MapBuilder*>(builder_ptr.get());
        auto* key_builder = static_cast<arrow::StringBuilder*>(mb->key_builder());
        int64_t rows = static_cast<int64_t>(cb.valid.size());
        switch (cb.kind) {
          case CK_MAP_STR_I64: {
            auto* value_builder = static_cast<arrow::Int64Builder*>(mb->item_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.m_off[r];
              int32_t end = cb.m_off[r + 1];
              if (!cb.valid[r]) {
                mb->AppendNull();
                continue;
              }
              mb->Append();
              for (int32_t e = begin; e < end; ++e) {
                int32_t key_start = cb.mk_soff[e];
                int32_t key_end = cb.mk_soff[e + 1];
                key_builder->Append(reinterpret_cast<const char*>(cb.mk_sdata.data() + key_start), key_end - key_start);
              }
              value_builder->AppendValues(cb.mv_i64.data() + begin, end - begin);
            }
            break;
          }
          case CK_MAP_STR_F32: {
            auto* value_builder = static_cast<arrow::FloatBuilder*>(mb->item_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.m_off[r];
              int32_t end = cb.m_off[r + 1];
              if (!cb.valid[r]) {
                mb->AppendNull();
                continue;
              }
              mb->Append();
              for (int32_t e = begin; e < end; ++e) {
                int32_t key_start = cb.mk_soff[e];
                int32_t key_end = cb.mk_soff[e + 1];
                key_builder->Append(reinterpret_cast<const char*>(cb.mk_sdata.data() + key_start), key_end - key_start);
              }
              value_builder->AppendValues(cb.mv_f32.data() + begin, end - begin);
            }
            break;
          }
          case CK_MAP_STR_STR: {
            auto* value_builder = static_cast<arrow::StringBuilder*>(mb->item_builder());
            for (int64_t r = 0; r < rows; ++r) {
              int32_t begin = cb.m_off[r];
              int32_t end = cb.m_off[r + 1];
              if (!cb.valid[r]) {
                mb->AppendNull();
                continue;
              }
              mb->Append();
              for (int32_t e = begin; e < end; ++e) {
                int32_t key_start = cb.mk_soff[e];
                int32_t key_end = cb.mk_soff[e + 1];
                key_builder->Append(reinterpret_cast<const char*>(cb.mk_sdata.data() + key_start), key_end - key_start);
                int32_t value_start = cb.mv_soff[e];
                int32_t value_end = cb.mv_soff[e + 1];
                value_builder->Append(reinterpret_cast<const char*>(cb.mv_sdata.data() + value_start), value_end - value_start);
              }
            }
            break;
          }
          default:
            break;
        }
        break;
      }
      case CK_MAP_STR_LIST_I64:
      case CK_MAP_STR_LIST_STR:
      case CK_MAP_STR_LIST_STRUCT:
        // Values were appended directly during decode via map builder.
        break;
      default:
        break;
    }
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays; arrays.reserve(schema->num_fields()); for (int i = 0; i < schema->num_fields(); ++i) { std::shared_ptr<arrow::Array> a; builders[i]->Finish(&a); arrays.push_back(std::move(a)); }
  auto batch = arrow::RecordBatch::Make(schema, (int64_t)n, arrays);
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr); auto* array_out = reinterpret_cast<ArrowArray*>(array_out_addr); arrow::ExportSchema(*schema.get(), schema_out); arrow::ExportRecordBatch(*batch.get(), array_out, schema_out);
}

// Minimal stub for direct ByteBuffer path; real implementation is integrated separately.
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
