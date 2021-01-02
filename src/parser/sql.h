//
// Created by Chen Qin on 12/30/20.
//

#ifndef SURFING_DB_SQL_H
#define SURFING_DB_SQL_H
#pragma once
#include <iostream>
#include <functional>
#include <rapidjson/document.h>


namespace surfingdb {
    namespace parser {
        using std::string;
        using rapidjson::Document;

        class SQLParser {
        public:
            SQLParser() = default;
            virtual ~SQLParser() = default;
            // read sql statement in string return syntax tree in json format
            void parser(const string& sqlstatement, Document& doc) noexcept;
            std::function<void()> interpret(const Document& doc) noexcept;
        };
    }
}
#endif //SURFING_DB_SQL_H
