//
// Created by cq on 1/7/21.
//

#include "Row.h"

namespace surfingdb {
namespace table {

RowBuffer::RowBuffer(RowSchema& schema) {
  _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
  _size = 0;
  for (size_t i = 0; i < schema.fields.size(); i++) {
    auto f = schema.fields.at(i);
    this->_offsets.get()->emplace(f, _size);
    if (f.type == RowType::MAP) {
      // don't know
    } else if (f.list_type != RowType::VOID) {
      _size += schema.fields.at(i).unit_size * (f.list_type == RowType::INT ? sizeof(int) : sizeof(long));
    } else {
      _size += schema.fields.at(i).unit_size;
    }
  }
  _payload = static_cast<uint8_t*>(malloc(_size));
}

void RowBuffer::read(Field& f, Value& v) {
  uint64_t offset = _offsets->at(f);
  switch (f.type) {
  case surfingdb::table::schema::RowType::VOID: {
    break;
  }
  case surfingdb::table::schema::RowType::INT: {
    int* int_ptr = (int*)(_payload + offset);
    v.p_val.int_val = *int_ptr;
    break;
  }
  case surfingdb::table::schema::RowType::BOOL: {
    bool* bool_ptr = (bool *)(_payload + offset);
    v.p_val.bool_val = *bool_ptr;
    break;
  }
  case RowType::LONG: {
    long* long_ptr = (long*)(_payload + offset);
    v.p_val.long_val = *long_ptr;
    break;
  }
  case RowType::DOUBLE: {
    double* doube_ptr = (double *)(_payload + offset);
    v.p_val.double_val = *doube_ptr;
    break;
  }
  case RowType::STRING: {
    // be careful here with truncation
    memcpy((char*)(_payload + offset), (v.p_val.string_val.c_str()), sizeof(char) * f.unit_size);
    break;
  }
  case RowType::LIST: {
    //hack here
    std::shared_ptr<char> vecotr_ptr((char*)(_payload + offset));
    //memcpy((void*) vecotr_ptr.get(), (void*)&v.list_value[0], s.Type_Size.at(f.list_type) * f.unit_size);
    break;
  }
  case RowType::MAP: {
    std::shared_ptr<char> map_ptr((char*)(_payload + offset));
    break;
  }
  }
  }

void RowBuffer::write(Field& f, Value& v) {
  uint64_t offset = _offsets->at(f);
  switch (f.type) {
  case surfingdb::table::schema::RowType::VOID: {
    break;
  }
  case surfingdb::table::schema::RowType::INT: {
    memcpy((int*)(_payload + offset), &(v.p_val.int_val), sizeof(int));
    break;
  }
  case surfingdb::table::schema::RowType::BOOL: {
    memcpy((bool*)(_payload + offset), &(v.p_val.bool_val), sizeof(bool));
    break;
  }
  case RowType::LONG: {
    memcpy((long*)(_payload + offset), &(v.p_val.long_val), sizeof(long));
    break;
  }
  case RowType::DOUBLE: {
    memcpy((double*)(_payload + offset), &(v.p_val.double_val), sizeof(double));
    break;
  }
  case RowType::STRING: {
    // be careful here with truncation
    memcpy((char*)(_payload + offset), (v.p_val.string_val.c_str()), sizeof(char) * f.unit_size);
    break;
  }
  case RowType::LIST: {
    //hack here
    std::shared_ptr<char> vecotr_ptr((char*)(_payload + offset));
    //memcpy((void*) vecotr_ptr.get(), (void*)&v.list_value[0], s.Type_Size.at(f.list_type) * f.unit_size);
    break;
  }
  case RowType::MAP: {
    std::shared_ptr<char> map_ptr((char*)(_payload + offset));
    break;
  }
  }
}
} // namespace table
} // namespace surfingdb