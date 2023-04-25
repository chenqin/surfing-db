/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SURFINGDB_UTILS_H
#define SURFINGDB_UTILS_H
#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/json/api.h>
#include <cstdarg>
#include <fcntl.h>
#include <future>
#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/api.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/io_util.h"

#include "KMeanOperator.h"
#include "frequent_items_sketch.hpp"
#include "mrow.h"
#include "xgbop.h"

#pragma once

namespace surfingdb {
namespace table {
using namespace surfingdb::meta;

typedef datasketches::frequent_items_sketch<std::string> frequent_strings_sketch;
/**
 * @brief convert surfing db internal data struct to arrow types and vice versa
 *
 */
class utils {
public:
  static void jvmGC(JNIEnv* env) {
    jclass systemClass = nullptr;
    jmethodID systemGCMethod = nullptr;
    // ...
    // Take out the trash.
    systemClass = env->FindClass("java/lang/System");
    systemGCMethod = env->GetStaticMethodID(systemClass, "gc", "()V");
    env->CallStaticVoidMethod(systemClass, systemGCMethod);
    CHECK(env->ExceptionCheck() == JNI_FALSE);
    env->DeleteLocalRef(systemClass);
  }
  static std::shared_ptr<arrow::ArrayBuilder> getBuilder(const RowType::type& type) {
    if (type == RowType::BOOL) {
      return std::make_shared<arrow::BooleanBuilder>();
    }
    if (type == RowType::CHAR) {
      return std::make_shared<arrow::Int8Builder>();
    }
    if (type == RowType::INT) {
      return std::make_shared<arrow::Int32Builder>();
    }
    if (type == RowType::LONG) {
      return std::make_shared<arrow::Int64Builder>();
    }
    if (type == RowType::DOUBLE) {
      return std::make_shared<arrow::DoubleBuilder>();
    }
    if (type == RowType::STRING) {
      return std::make_shared<arrow::StringBuilder>();
    }
    return std::make_shared<arrow::NullBuilder>();
  }

  static arrow::Status append(arrow::ArrayBuilder* builder, const Field& field, const PValue& p_val, const Value& v) {
    if (field.type == RowType::BOOL) {
      ARROW_RETURN_NOT_OK(((arrow::BooleanBuilder*)builder)->Append(p_val.bool_val));
    }
    if (field.type == RowType::INT) {
      ARROW_RETURN_NOT_OK(((arrow::Int32Builder*)builder)->Append(p_val.int_val));
    }
    if (field.type == RowType::CHAR) {
      ARROW_RETURN_NOT_OK(((arrow::Int8Builder*)builder)->Append(p_val.byte_val));
    }
    if (field.type == RowType::LONG) {
      ARROW_RETURN_NOT_OK(((arrow::Int64Builder*)builder)->Append(p_val.long_val));
    }
    if (field.type == RowType::DOUBLE) {
      ARROW_RETURN_NOT_OK(((arrow::DoubleBuilder*)builder)->Append(p_val.double_val));
    }
    if (field.type == RowType::STRING) {
      ARROW_RETURN_NOT_OK(((arrow::StringBuilder*)builder)->Append(p_val.string_val));
    }
    if (field.type == RowType::LIST) {
      auto list_builder = (arrow::ListBuilder*)builder;
      ARROW_RETURN_NOT_OK(list_builder->Append());

      auto e_builder = list_builder->value_builder();
      Field lfield;
      lfield.type = field.list_type;
      for (auto m : v.list_value) {
        ARROW_RETURN_NOT_OK(append(e_builder, lfield, m, v));
      }
    }
    if (field.type == RowType::MAP) {
      auto map_builder = (arrow::MapBuilder*)builder;
      ARROW_RETURN_NOT_OK(map_builder->Append());
      auto k_builder = map_builder->key_builder();
      auto v_builder = map_builder->item_builder();
      Field kfield, vfield;
      kfield.type = field.map_key_type;
      vfield.type = field.map_value_type;
      for (auto n : v.map_value) {
        ARROW_RETURN_NOT_OK(append(k_builder, kfield, n.first, v));
        ARROW_RETURN_NOT_OK(append(v_builder, vfield, n.second, v));
      }
    }
    return arrow::Status::OK();
  }

  /**
   * convert to arrow schema types
   */
  static auto getArrowType(const RowType::type& type, const RowType::type& type1, const RowType::type& type2, size_t units) {
    if (type == RowType::BOOL) {
      return arrow::boolean();
    } else if (type == RowType::CHAR) {
      return arrow::int8();
    } else if (type == RowType::INT) {
      return arrow::int32();
    } else if (type == RowType::LONG) {
      return arrow::int64();
    } else if (type == RowType::DOUBLE) {
      return arrow::float32();
    } else if (type == RowType::STRING) {
      return arrow::utf8();
    } else if (type == RowType::LIST) {
      return arrow::list(getArrowType(type1, RowType::VOID, RowType::VOID, 1));
    } else if (type == RowType::MAP) {
      return arrow::map(getArrowType(type1, RowType::VOID, RowType::VOID, 1), getArrowType(type2, RowType::VOID, RowType::VOID, 1), true);
    } else {
      return arrow::null();
    }
  }

  static RowType::type getRowType(std::shared_ptr<arrow::DataType> type) {
    switch (type->id()) {
    case arrow::Type::BOOL:
      return RowType::BOOL;
    case arrow::Type::INT8:
      return RowType::CHAR;
    case arrow::Type::INT32:
    case arrow::Type::INT16:
      return RowType::INT;
    case arrow::Type::INT64:
      return RowType::LONG;
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
      return RowType::DOUBLE;
    case arrow::Type::STRING:
      return RowType::STRING;
    default:
      return RowType::VOID;
    }
  }

  static std::shared_ptr<arrow::Schema> toArrow(const std::shared_ptr<mschema> schema) {
    /**
     * arrow schema conversion
     */
    std::vector<std::shared_ptr<arrow::Field>> field_vector;
    for (auto j = 0; j < schema->fields.size(); j++) {
      auto fj = schema->fields.at(j);
      auto units = fj.max_unit_size;
      std::shared_ptr<arrow::Field> afield;
      if (fj.type == RowType::MAP) {
        afield = arrow::field(fj.name, getArrowType(fj.type, fj.map_key_type, fj.map_value_type, units));
      } else if (fj.type == RowType::LIST) {
        afield = arrow::field(fj.name, getArrowType(fj.type, fj.list_type, RowType::VOID, units));
      } else {
        afield = arrow::field(fj.name, getArrowType(fj.type, RowType::VOID, RowType::VOID, 1));
      }
      field_vector.push_back(afield);
    }
    return arrow::schema(field_vector);
  }

  /**
   * @brief convert arrow schema with unit size map as mschema
   * @param schema
   * @param units
   * @return std::shared_ptr<mschema>
   */
  static std::shared_ptr<mschema> fromArrow(std::shared_ptr<arrow::Schema> schema, std::map<std::string, uint64_t>& units) {
    RowSchema r;
    for (int i = 0; i < schema->num_fields(); i++) {
      auto field = schema->field(i);
      auto id = field->type()->id();
      auto max_unit = units.find(field->name()) == units.end() ? MAX_STR_LEN : units[field->name()];
      if (id == arrow::Type::LIST) {
        auto ltype = (arrow::ListType*)field->type().get();
        auto type = getRowType(ltype->value_type());
        auto unit_size = units.find(field->name() + "#") == units.end() ? MAX_STR_LEN : units[field->name() + "#"];
        SchemaUtils::initListField(r, field->name(), type, unit_size, max_unit);
      } else if (id == arrow::Type::MAP) {
        auto mtype = (arrow::MapType*)field->type().get();
        auto key_unit_size = units.find(field->name() + "#1") == units.end() ? MAX_STR_LEN : units[field->name() + "#1"];
        auto val_unit_size = units.find(field->name() + "#2") == units.end() ? MAX_STR_LEN : units[field->name() + "#2"];
        SchemaUtils::initMapField(r, field->name(), getRowType(mtype->key_type()), getRowType(mtype->item_type()), max_unit, key_unit_size, val_unit_size);
      } else {
        auto type = getRowType(field->type());
        SchemaUtils::initField(r, field->name(), type, max_unit);
      }
    }
    return std::make_shared<mschema>(r);
  }

  static std::map<std::string, uint64_t> toUnits(std::vector<std::shared_ptr<arrow::RecordBatch>> records) {
    std::map<std::string, uint64_t> units;

    for (auto& record_ptr : records) {
      auto vc = record_ptr->columns();
      /**
       * @brief iterate all rows in arrow recordbatch
       *
       * @param i
       */
      // CHECK(record_ptr->num_rows() > 0);
      for (int64_t i = 0; i < record_ptr->num_rows(); i++) {
        for (auto j = 0; j < vc.size(); j++) {
          auto field = record_ptr->schema()->field(j);
          auto col = record_ptr->column(j);

          auto result = col->GetScalar(i).ValueOrDie();
          Value v;
          auto type = field->type()->id();
          auto name = field->name();

          if (type == arrow::Type::STRING) {
            auto bc = (arrow::StringScalar*)result.get();
            if (auto search = units.find(name); search != units.end()) {
              units[name] = units[name] > bc->ToString().length() ? units[name] : bc->ToString().length();
            } else {
              units[name] = bc->ToString().length();
            }
          }
          if (type == arrow::Type::LIST) {
            auto bc = (arrow::ListScalar*)result.get();
            auto list = bc->value;
            units[name] = list->length();
            int max_str_len = 0;
            for (int k = 0; k < list->length(); k++) {
              auto item = list->GetScalar(k).ValueOrDie();
              if (list->type_id() == arrow::Type::STRING) {
                auto bc = (arrow::StringScalar*)item.get();
                int len = bc->ToString().length();
                max_str_len = len > max_str_len ? len : max_str_len;
              }
            }
            units[name + "#"] = max_str_len;
          }
          if (type == arrow::Type::MAP) {
            auto bc = (arrow::MapScalar*)result.get();
            units[name] = bc->value->length();
            size_t key_len = 0;
            size_t val_len = 0;
            for (int k = 0; k < bc->value->length(); k++) {
              auto pairval = bc->value->GetScalar(k).ValueOrDie();
              auto structval = (arrow::StructScalar*)pairval.get();
              std::shared_ptr<arrow::Scalar> keyval = structval->field("key").ValueOrDie();
              std::shared_ptr<arrow::Scalar> itemval = structval->field("value").ValueOrDie();
              PValue keyp, valuep;
              writeV(valuep, itemval->type->id(), itemval.get());

              if (keyval->type->id() == arrow::Type::STRING) {
                auto bc = (arrow::StringScalar*)keyval.get();
                int len = bc->ToString().length();
                key_len = len > key_len ? len : key_len;
              }
              if (itemval->type->id() == arrow::Type::STRING) {
                auto bc = (arrow::StringScalar*)itemval.get();
                int len = bc->ToString().length();
                val_len = len > val_len ? len : val_len;
              }
            }
            units[name + "#1"] = key_len;
            units[name + "#2"] = val_len;
          }
        }
      }
    }
    size_t unit_max[units.size()];
    int i = 0;
    for (auto u : units) {
      unit_max[i++] = u.second;
    }
    size_t global_unit_max[units.size()];
    MPI_Allreduce(&unit_max, &global_unit_max, units.size(), MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);
    i = 0;
    for (auto u : units) {
      u.second = global_unit_max[i++] + 1;
    }
    return units;
  }

  static std::vector<std::shared_ptr<arrow::RecordBatch>> hash(std::vector<std::shared_ptr<arrow::RecordBatch>>& batch, std::string field_name, std::function<size_t(size_t, int, int)>& partitioner, int rank, int world) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_tables;
    for (auto& b : batch) {
      arrow::Int8Builder hash_builder;
      for (int i = 0; i < b->num_rows(); i++) {
        CHECK(b->GetColumnByName(field_name)->GetScalar(i).ok());
        auto v = b->GetColumnByName(field_name)->GetScalar(i).ValueOrDie();
        auto dest_rank = partitioner(v->hash(), rank, world);
        hash_builder.Append(dest_rank);
      }
      if (b->num_rows() > 0) {
        std::shared_ptr<arrow::Array> _array;
        hash_builder.Finish(&_array);
        auto result = b->AddColumn(b->num_columns(), "_hash", _array);
        CHECK(result.ok());
        arrow_tables.push_back(result.ValueOrDie());
      }
    }
    return arrow_tables;
  }

  static std::shared_ptr<arrow::RecordBatch> group(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batch, std::string field_name, std::function<size_t(size_t, int, int)>& partitioner, int dest, int rank, int world) {
    CHECK_GT(batch.size(), 0);
    auto schema = batch[0]->schema();

    std::vector<std::shared_ptr<arrow::ArrayBuilder>> array_builders;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    size_t count = 0;

    for (auto i = 0; i < schema->fields().size(); i++) {
      auto field = schema->field(i);
      switch (field->type()->id()) {

      case arrow::Type::BOOL: {
        std::shared_ptr<arrow::BooleanBuilder> builder = std::make_shared<arrow::BooleanBuilder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::INT8: {
        std::shared_ptr<arrow::Int8Builder> builder = std::make_shared<arrow::Int8Builder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::INT32: {
        std::shared_ptr<arrow::Int32Builder> builder = std::make_shared<arrow::Int32Builder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::INT64: {
        std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::FLOAT: {
        std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::DOUBLE: {
        std::shared_ptr<arrow::DoubleBuilder> builder = std::make_shared<arrow::DoubleBuilder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::BINARY: {
        std::shared_ptr<arrow::BinaryBuilder> builder = std::make_shared<arrow::BinaryBuilder>();
        array_builders.push_back(builder);
        break;
      }

      case arrow::Type::STRING: {
        std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
        array_builders.push_back(builder);
        break;
      }
      // Add cases for other data types as needed.
      default: {
        throw std::runtime_error("Unsupported data type");
      }
      }
    }

    for (auto& b : batch) {
      for (int i = 0; i < b->num_rows(); i++) {
        CHECK(b->GetColumnByName(field_name)->GetScalar(i).ok());
        auto v = b->GetColumnByName(field_name)->GetScalar(i).ValueOrDie();
        auto dest_rank = partitioner(v->hash(), rank, world);
        if (dest_rank == dest) {
          count++;
          for (int j = 0; j < b->num_columns(); j++) {
            CHECK(b->column(j)->GetScalar(i).ok());
            auto v = b->column(j)->GetScalar(i).ValueOrDie();
            auto builder = array_builders.at(j);
            builder->AppendScalar(*v.get());
          }
        }
      }
    }

    for (auto& builder : array_builders) {
      std::shared_ptr<arrow::Array> array;
      CHECK(builder->Finish(&array).ok());
      arrays.push_back(array);
    }
    return arrow::RecordBatch::Make(schema, count, arrays);
  }

  static std::shared_ptr<mtable> fromArrow(std::vector<std::shared_ptr<arrow::RecordBatch>> records, std::map<std::string, uint64_t>& units, std::shared_ptr<node> node_ptr) {
    CHECK(records.size() > 0);
    auto schema = fromArrow(records.at(0)->schema(), units);

    size_t row_count = 0;

    for (auto& record : records) {
      row_count += record->num_rows();
    }
    auto table = node_ptr == nullptr ? std::make_shared<mtable>(schema, row_count * schema->rowSize()) : std::make_shared<mtable>(node_ptr, schema, row_count * schema->rowSize());
    CHECK(schema->fields.size() > 0);

    for (auto& record_ptr : records) {
      auto vc = record_ptr->columns();
      /**
       * @brief iterate all rows in arrow recordbatch
       *
       * @param i
       */
      for (auto i = 0; i < record_ptr->num_rows(); i++) {

        mrow r(schema);
        /**
         * @brief iterate all fields in schema with j
         *
         */
        for (auto j = 0; j < vc.size(); j++) {
          auto field = record_ptr->schema()->field(j);
          auto col = record_ptr->column(j);

          auto result = col->GetScalar(i).ValueOrDie();
          Value v;
          auto type = field->type()->id();

          if (type == arrow::Type::BOOL) {
            auto bc = (arrow::BooleanScalar*)result.get();
            v.p_val.bool_val = bc->value;
          }
          if (type == arrow::Type::INT8) {
            auto bc = (arrow::Int8Scalar*)result.get();
            v.p_val.byte_val = bc->value;
          }
          if (type == arrow::Type::INT32) {
            auto bc = (arrow::Int32Scalar*)result.get();
            v.p_val.int_val = bc->value;
          }
          if (type == arrow::Type::INT64) {
            auto bc = (arrow::Int64Scalar*)result.get();
            v.p_val.long_val = bc->value;
          }
          if (type == arrow::Type::FLOAT) {
            auto bc = (arrow::FloatScalar*)result.get();
            v.p_val.double_val = bc->value;
          }
          if (type == arrow::Type::STRING) {
            auto bc = (arrow::StringScalar*)result.get();
            v.p_val.string_val = bc->ToString();
          }
          if (type == arrow::Type::LIST) {
            auto bc = (arrow::ListScalar*)result.get();
            auto list = bc->value;
            CHECK_LE(list->length(), schema->fields.at(j).max_unit_size);
            for (int k = 0; k < list->length(); k++) {
              auto item = list->GetScalar(k).ValueOrDie();
              PValue pval;
              writeV(pval, list->type_id(), item.get());
              v.list_value.push_back(pval);
            }
          }
          if (type == arrow::Type::MAP) {
            auto bc = (arrow::MapScalar*)result.get();
            /**
             * @brief insert number of items in map
             *
             */
            CHECK_LE(bc->value->length(), schema->fields.at(j).max_unit_size);
            for (int k = 0; k < bc->value->length(); k++) {
              auto pairval = bc->value->GetScalar(k).ValueOrDie();
              auto structval = (arrow::StructScalar*)pairval.get();
              std::shared_ptr<arrow::Scalar> keyval = structval->field("key").ValueOrDie();
              std::shared_ptr<arrow::Scalar> itemval = structval->field("value").ValueOrDie();
              PValue keyp, valuep;
              writeV(keyp, keyval->type->id(), keyval.get());
              writeV(valuep, itemval->type->id(), itemval.get());
              v.map_value.insert({ keyp, valuep });
            }
          }
          /**
           * @brief write j field to row
           *
           */
          r.write(schema->fields.at(j), v);
        }
        table->appendRow(r);
      }
    }
    return table;
  }

  static void writeV(PValue& v, arrow::Type::type type, arrow::Scalar* s) {
    if (s == nullptr) return;
    if (type == arrow::Type::BOOL) {
      auto bc = (arrow::BooleanScalar*)s;
      v.bool_val = bc->value;
    }
    if (type == arrow::Type::INT8) {
      auto bc = (arrow::Int8Scalar*)s;
      v.byte_val = bc->value;
    }
    if (type == arrow::Type::INT32) {
      auto bc = (arrow::Int32Scalar*)s;
      v.int_val = bc->value;
    }
    if (type == arrow::Type::INT64) {
      auto bc = (arrow::Int64Scalar*)s;
      v.long_val = bc->value;
    }
    if (type == arrow::Type::FLOAT) {
      auto bc = (arrow::FloatScalar*)s;
      v.double_val = bc->value;
    }
    if (type == arrow::Type::STRING) {
      auto bc = (arrow::StringScalar*)s;
      v.string_val = bc->ToString();
    }
  }

  static std::shared_ptr<arrow::RecordBatch> placeholder(std::shared_ptr<arrow::Schema> schema, std::map<std::string, uint64_t>& units) {
    auto schema_ptr = fromArrow(schema, units);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    for (auto i = 0; i < schema_ptr->fields.size(); i++) {
      auto type = schema_ptr->fields.at(i).type;
      if (type == RowType::LIST) {
        auto keytype = schema_ptr->fields.at(i).list_type;
        builders.push_back(std::make_shared<arrow::ListBuilder>(pool, getBuilder(keytype)));
        continue;
      }
      if (type == RowType::MAP) {
        auto keytype = schema_ptr->fields.at(i).map_key_type;
        auto valuetype = schema_ptr->fields.at(i).map_value_type;
        builders.push_back(std::make_shared<arrow::MapBuilder>(pool, getBuilder(keytype), getBuilder(valuetype)));
        continue;
      }
      builders.push_back(getBuilder(type));
    }

    CHECK(schema_ptr->fields.size() == builders.size());
    for (auto k = 0; k < schema_ptr->fields.size(); k++) {
      auto field = schema_ptr->fields.at(k);
      auto builder_ptr = builders.at(k);
      Value v;
      append(builder_ptr.get(), field, v.p_val, v);
    }

    for (auto b : builders) {
      std::shared_ptr<arrow::Array> _array;
      b->Finish(&_array);
      arrays.push_back(_array);
    }
    return arrow::RecordBatch::Make(schema, 1, arrays);
  }

  static std::shared_ptr<arrow::Buffer> serializeSchema(std::shared_ptr<arrow::Schema> schema) {
    return arrow::ipc::SerializeSchema(*schema.get()).ValueOrDie();
  }

  static std::shared_ptr<arrow::Schema> deserializeSchema(std::shared_ptr<arrow::Buffer> buffer) {
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo empty_memo;
    return arrow::ipc::ReadSchema(&reader, &empty_memo).ValueOrDie();
  }

  static std::shared_ptr<arrow::Buffer> serialize(std::shared_ptr<arrow::RecordBatch> batch) {
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    return arrow::ipc::SerializeRecordBatch(*batch.get(), options).ValueOrDie();
  }

  static std::shared_ptr<arrow::RecordBatch> deserialize(std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<arrow::Schema> schema) {
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo empty_memo;
    return arrow::ipc::ReadRecordBatch(schema, &empty_memo,
                                       arrow::ipc::IpcReadOptions::Defaults(), &reader)
      .ValueOrDie();
  }

  static std::shared_ptr<arrow::RecordBatch> toArrow(const std::shared_ptr<surfingdb::table::mtable> table) {
    /**
     * Build list of builders to append
     */
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    for (auto i = 0; i < table->getSchema()->fields.size(); i++) {
      auto type = table->getSchema()->fields.at(i).type;
      if (type == RowType::LIST) {
        auto keytype = table->getSchema()->fields.at(i).list_type;
        builders.push_back(std::make_shared<arrow::ListBuilder>(pool, getBuilder(keytype)));
        continue;
      }
      if (type == RowType::MAP) {
        auto keytype = table->getSchema()->fields.at(i).map_key_type;
        auto valuetype = table->getSchema()->fields.at(i).map_value_type;
        builders.push_back(std::make_shared<arrow::MapBuilder>(pool, getBuilder(keytype), getBuilder(valuetype)));
        continue;
      }
      builders.push_back(getBuilder(type));
    }

    /**
     * read each row and field value, append to corresponding builder, release mtable vector<mrow>
     */
    for (auto j = 0; j < table->row_count; j++) {
      auto r = table->readRow(j);
      CHECK(table->getSchema()->fields.size() == builders.size());
      for (auto k = 0; k < table->getSchema()->fields.size(); k++) {
        auto field = table->getSchema()->fields.at(k);
        auto builder_ptr = builders.at(k);
        Value v;
        r->read(field, v);
        append(builder_ptr.get(), field, v.p_val, v);
      }
    }

    for (auto b : builders) {
      std::shared_ptr<arrow::Array> _array;
      b->Finish(&_array);
      arrays.push_back(_array);
    }
    return arrow::RecordBatch::Make(toArrow(table->getSchema()), table->row_count, arrays);
  }
};

} // namespace table
} // namespace surfingdb
#endif // SURFINGDB_UTILS_H
