//
// Created by Chen Qin on 12/30/20.
//

#include "sql.h"
#include <pg_query.h>
#include <glog/logging.h>

namespace surfingdb {
    namespace parser {

        void SQLParser::parser(const string& sqlstatement, Document& doc) noexcept {
            auto postgresSQL = pg_query_parse(sqlstatement.c_str());
            if (doc.Parse(postgresSQL.parse_tree).HasParseError() || doc.GetArray().Empty()) {
                LOG(ERROR) << postgresSQL.parse_tree << "ddl has syntax error use this https://rextester.com/l/postgresql_online_compiler";
                return;
            }
            LOG(INFO) << postgresSQL.parse_tree;
            pg_query_free_parse_result(postgresSQL);
        }

        void SQLParser::interpret(const Document &doc) noexcept {
            // we only need one pass to get udfs
            // table->udfs.clear();

            // track permutation of locations by replace macro(s) in table location
            std::vector<std::string> locations;
            //locations.push_back(table->location);

            for (auto& statements : doc.GetArray()) {
                auto root = statements.GetObject();
                auto head = root.FindMember("RawStmt")->value.GetObject().FindMember("stmt")->value.GetObject();
                bool isView = head.FindMember("ViewStmt") != head.MemberEnd();
                bool isSelect = head.FindMember("SelectStmt") != head.MemberEnd();
                assert(isSelect || isView);

                // extract view namespace and name
                if (isView) {
                    auto RangeVar = head.FindMember("ViewStmt")->value.GetObject().FindMember("view")->value.GetObject().FindMember("RangeVar")->value.GetObject();
                    auto ns = RangeVar.FindMember("schemaname")->value.GetString();
                    auto db = RangeVar.FindMember("relname")->value.GetString();
                    // LOG(INFO) << "view name " << ns << "." << db;
                    // assign name
                    LOG(INFO) << std::string(ns).append(db);
                }

                // extract columns and UDFs
                // for view statement, we need to ViewStmt/query/SelectStmt
                auto selectStmt = isView ? head.FindMember("ViewStmt")->value.GetObject().FindMember("query")->value.GetObject().FindMember("SelectStmt")->value.GetObject() : head.FindMember("SelectStmt")->value.GetObject();
                auto columns = selectStmt.FindMember("targetList")->value.GetArray();

                for (auto& c : columns) {
                    auto in = c.GetObject();
                    bool hasAlias = in.FindMember("ResTarget")->value.GetObject().FindMember("name") != in.FindMember("ResTarget")->value.GetObject().MemberEnd();
                    auto val = in.FindMember("ResTarget")->value.GetObject().FindMember("val")->value.GetObject();
                    bool isFuncCall = val.FindMember("FuncCall") != val.MemberEnd();
                    bool isTypeCast = val.FindMember("TypeCast") != val.MemberEnd();
                    bool isColumn = val.FindMember("ColumnRef") != val.MemberEnd();
                    assert(isFuncCall || isColumn || isTypeCast);
                    std::string alias = hasAlias ? in.FindMember("ResTarget")->value.GetObject().FindMember("name")->value.GetString() : "";

                    if (isColumn) {
                        auto fields = val.FindMember("ColumnRef")->value.FindMember("fields")->value.GetArray();
                        for (auto& f : fields) {
                            auto fieldName = f.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
                            std::pair<std::string, std::string> col("", fieldName);
                            LOG(INFO) << col.first << col.second;
                        }
                    } else if (isFuncCall) {
                        auto funcs = val.FindMember("FuncCall")->value.FindMember("funcname")->value.GetArray();
                        auto args = val.FindMember("FuncCall")->value.FindMember("args")->value.GetArray();
                        rapidjson::Value ::ConstValueIterator funcitr = funcs.Begin();
                        rapidjson::Value ::ConstValueIterator argitr = args.Begin();
                        while (funcitr != funcs.End()) {
                            auto funcName = funcitr->GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();

                            std::string argument_list = "";
                            // function without arguments
                            if (argitr != args.End()) {
                                bool isMulitValue = argitr->GetObject().FindMember("A_Expr") != argitr->GetObject().MemberEnd();
                                // TODO(chenqin): only support MYUDF(field) should support UDF involving multi fields e.g MYUDF(id + flag),
                                assert(!isMulitValue);
                                auto fields = argitr->GetObject().FindMember("ColumnRef")->value.FindMember("fields")->value.GetArray();

                                for (auto& f : fields) {
                                    auto fieldName = f.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
                                    argument_list.append(fieldName);
                                }
                                argitr++;
                            }
                            std::pair<std::string, std::string> col(funcName, argument_list);
                            LOG(INFO) << col.first << col.second;
                            funcitr++;
                        }
                    } else if (isTypeCast) {
                        auto args = val.FindMember("TypeCast")->value.FindMember("arg")->value.FindMember("ColumnRef")->value.FindMember("fields")->value.GetArray();
                        auto typeNameField = val.FindMember("TypeCast")->value.FindMember("typeName")->value.FindMember("TypeName")->value.FindMember("names")->value.GetArray();

                        std::string typeName; //full name of type cast to e.g pg_catalog.int8
                        for (const auto& na : typeNameField) {
                            auto name = na.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
                            typeName.append(typeName.empty() ? name : "." + std::string(name));
                        }

                        for (auto& f : args) {
                            auto fieldName = f.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
                            std::pair<std::string, std::string> col("cast " + typeName + " as " + alias, fieldName);
                            LOG(INFO) << col.first << col.second;
                        }
                    }
                }

                auto fromTables = selectStmt.FindMember("fromClause")->value.GetArray();
                for (auto& t : fromTables) {
                    auto name = t.GetObject().FindMember("RangeVar")->value.GetObject().FindMember("relname")->value.GetString();
                    LOG(INFO) << "from table " << name;
                }

                auto whereClauses = selectStmt.FindMember("whereClause")->value.GetObject();
                bool boolExpr = whereClauses.FindMember("BoolExpr") != whereClauses.MemberEnd();
                // only support boolexp for now
                assert(boolExpr);

                auto arguments = whereClauses.FindMember("BoolExpr")->value.GetObject().FindMember("args")->value.GetArray();
                for (auto& arg : arguments) {
                    std::string config_name, config_ref;
                    std::string config_val; // String config_val
                    int iconfig_val = -1;        // Integer config_val
                    float fconfig_val = -1L;      // Float config_val

                    bool isAExpr = arg.GetObject().FindMember("A_Expr") != arg.GetObject().MemberEnd();
                    assert(isAExpr);

                    auto aExpr = arg.GetObject().FindMember("A_Expr")->value.GetObject();
                    auto names = aExpr.FindMember("name")->value.GetArray();

                    for (auto& nm : names) {
                        auto exp = nm.FindMember("String")->value.FindMember("str")->value.GetString();
                        config_ref.append(config_ref.empty() ? exp : "." + std::string(exp));
                    }

                    auto lexpr = aExpr.FindMember("lexpr")->value.GetObject();
                    auto columnRef = lexpr.FindMember("ColumnRef")->value.GetObject();
                    auto fields = columnRef.FindMember("fields")->value.GetArray();
                    for (auto& f : fields) {
                        auto exp = f.FindMember("String")->value.FindMember("str")->value.GetString();
                        config_name.append(config_name.empty() ? exp : "." + std::string(exp));
                    }

                    const bool isMacro = config_name.find("macro.") != std::string::npos;

                    bool arrRExpr = aExpr.FindMember("rexpr")->value.IsArray();
                    if (arrRExpr) {
                        auto rexprs = aExpr.FindMember("rexpr")->value.GetArray();
                        // handle collection xx in (aa, bb)
                        for (auto& expr : rexprs) {
                            bool aConst = expr.FindMember("A_Const") != expr.MemberEnd();
                            if (aConst) {
                                auto val = expr.FindMember("A_Const")->value.GetObject().FindMember("val")->value.GetObject();
                                bool isFloat = val.FindMember("Float") != val.MemberEnd();
                                bool isString = val.FindMember("String") != val.MemberEnd();
                                bool isInteger = val.FindMember("Integer") != val.MemberEnd();
                                assert(isFloat || isString || isInteger);
                                if (isInteger) {
                                    auto ival = val.FindMember("Integer")->value.FindMember("ival")->value.GetInt();
                                    iconfig_val = ival;
                                } else {
                                    auto constval = val.FindMember(isFloat ? "Float" : "String")->value.FindMember("str")->value.GetString();
                                    config_val = constval;
                                    if (isFloat) {
                                        fconfig_val = std::stof(constval);
                                    }
                                }
                                // recursive replace location macro and call interpret
                                if (isMacro) {
                                    // do str matching requires convert to string
                                    const auto macro = "%7B" + config_name.substr(6) + "%7D";
                                    for (auto s : locations) {
                                        if (s.find(macro) != std::string::npos) {
                                            std::string path(s);
                                            std::string macroValue = isInteger ? std::to_string(iconfig_val) : config_val;
                                            //replace(path, macro, macroValue);
                                            LOG(INFO) << "replacing macro " << macro << " with: " << path;
                                            //paths.push_back(path);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        auto rexpr = aExpr.FindMember("rexpr")->value.GetObject();
                        // hack, copy code from above
                        bool aConst = rexpr.FindMember("A_Const") != rexpr.MemberEnd();
                        if (aConst) {
                            auto val = rexpr.FindMember("A_Const")->value.GetObject().FindMember("val")->value.GetObject();
                            bool isFloat = val.FindMember("Float") != val.MemberEnd();
                            bool isString = val.FindMember("String") != val.MemberEnd();
                            bool isInteger = val.FindMember("Integer") != val.MemberEnd();
                            assert(isFloat || isString || isInteger);
                            if (isInteger) {
                                auto ival = val.FindMember("Integer")->value.FindMember("ival")->value.GetInt();
                                // LOG(INFO) << "rexpr : " << ival;
                                iconfig_val = ival;
                            } else {
                                auto constval = val.FindMember(isFloat ? "Float" : "String")->value.FindMember("str")->value.GetString();
                                //LOG(INFO) << "rexpr : " << constval;
                                config_val = constval;
                                if (isFloat) {
                                    fconfig_val = std::stof(constval);
                                }
                            }
                        }
                    }
                    LOG(INFO) << config_name << config_val << iconfig_val << fconfig_val;
                    //::nebula::ingest::assign(table, config_name, config_val, iconfig_val, fconfig_val);
                    if (isMacro) {
                        //locations = paths;
                        //paths.clear();
                    }
                }
            }

            // if s3 location empty, need extra loop get table->location assigned before interpret
           // if (table->location.empty() && table->source == DataSource::S3) {
           //     interpret(version, table, locations, doc);
           //     return;
            //}
            //paths = std::move(locations);
        }
    }
}