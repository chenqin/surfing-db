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

#ifndef SURFING_DB_SQL_H
#define SURFING_DB_SQL_H
#pragma once
#include <functional>
#include <iostream>
#include <rapidjson/document.h>

namespace surfingdb {
namespace parser {
using rapidjson::Document;
using std::string;

class SQLParser {
public:
  SQLParser() = default;
  virtual ~SQLParser() = default;
  // read sql statement in string return syntax tree in json format
  void parser(const string& sqlstatement, Document& doc) noexcept;
  std::function<void()> interpret(const Document& doc) noexcept;
};
} // namespace parser
} // namespace surfingdb
#endif //SURFING_DB_SQL_H
