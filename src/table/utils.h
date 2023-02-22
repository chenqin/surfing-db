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
#include <cstdarg>
#include <fcntl.h>
#include <future>
#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>

#include "KMeanOperator.h"
#include "mrow.h"
#include "xgbop.h"

#pragma once

namespace surfingdb {
namespace table {
using namespace surfingdb::meta;

/**
 * @brief convert surfing db internal data struct to arrow types and vice versa
 *
 */
class utils {
private:
  static std::shared_ptr<arrow::ArrayBuilder> getBuilder(const RowType::type& type) {
    if (type == RowType::BOOL) {
      return std::make_shared<arrow::BooleanBuilder>();
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
  static auto getArrowType(const RowType::type& type, const RowType::type& type1, const RowType::type& type2) {
    if (type == RowType::BOOL) {
      return arrow::boolean();
    } else if (type == RowType::INT) {
      return arrow::int32();
    } else if (type == RowType::LONG) {
      return arrow::int64();
    } else if (type == RowType::DOUBLE) {
      return arrow::float32();
    } else if (type == RowType::STRING) {
      return arrow::utf8();
    } else if (type == RowType::LIST) {
      return arrow::list(getArrowType(type1, RowType::VOID, RowType::VOID));
    } else if (type == RowType::MAP) {
      return arrow::map(getArrowType(type1, RowType::VOID, RowType::VOID), getArrowType(type2, RowType::VOID, RowType::VOID), true);
    } else {
      return arrow::null();
    }
  }

public:
  static std::shared_ptr<arrow::Schema> toArrow(const std::shared_ptr<mschema> schema) {
    /**
     * arrow schema conversion
     */
    std::vector<std::shared_ptr<arrow::Field>> field_vector;
    for (auto j = 0; j < schema->fields.size(); j++) {
      auto fj = schema->fields.at(j);
      std::shared_ptr<arrow::Field> afield;
      if (fj.type == RowType::MAP) {
        afield = arrow::field(fj.name, getArrowType(fj.type, fj.map_key_type, fj.map_value_type));
      } else if (fj.type == RowType::LIST) {
        afield = arrow::field(fj.name, getArrowType(fj.type, fj.list_type, RowType::VOID));
      } else {
        afield = arrow::field(fj.name, getArrowType(fj.type, RowType::VOID, RowType::VOID));
      }
      field_vector.push_back(afield);
    }
    return arrow::schema(field_vector);
  }

  static std::shared_ptr<mschema> fromArrow(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<node> node_ptr) {
    return nullptr;
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
    return arrow::RecordBatch::Make(toArrow(table->getSchema()), arrays.size(), arrays);
  }

  static std::shared_ptr<surfingdb::table::mtable> fromArrow(const std::shared_ptr<arrow::RecordBatch> table, std::shared_ptr<node> node_ptr) {
    return nullptr;
  }
};

} // namespace table
} // namespace surfingdb
#endif // SURFINGDB_UTILS_H
