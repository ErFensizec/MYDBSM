/*
--------------------

version 2.15

--------------------
*/
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <regex>
#include <fstream>
#include <nlohmann/json.hpp>
#include <map>
#include <unordered_set>
#include <algorithm>
#include <set>
#include <filesystem>
#include "DBMAKER.h"
#include "calculator2.h"

using namespace std;
using json = nlohmann::json;

namespace fs = std::filesystem;
/*
// 字段结构体
struct Field {
    string name;
    string type;
    string keyFlag;
    string nullFlag;
    string validFlag;
};

// 表结构
class Table {
public:
    string tableName;
    string databaseName;
    int fieldCount;
    vector<Field> fields;

    void print() const {
        cout << "Table Name: " << tableName << endl;
        cout << "Database File: " << databaseName << endl;
        for (const auto& field : fields) {
            cout << "Field: " << field.name
                << ", Type: " << field.type
                << ", Key: " << field.keyFlag
                << ", Null: " << field.nullFlag
                << ", Valid: " << field.validFlag << endl;
        }
    }

    json toJson() const {
        json j;
        j["table_name"] = tableName;
        j["database_name"] = databaseName;
        for (const auto& field : fields) {
            json fieldJson;
            fieldJson["name"] = field.name;
            fieldJson["type"] = field.type;
            fieldJson["key_flag"] = field.keyFlag;
            fieldJson["null_flag"] = field.nullFlag;
            fieldJson["valid_flag"] = field.validFlag;
            j["fields"].push_back(fieldJson);
        }
        return j;
    }
};*/

map<string, vector<Table>> databaseTables;
vector<string> results;
ostringstream output;

// 从 JSON 文件加载数据库表信息
map<string, vector<Table>> loadDatabaseTables(const string& jsonFilePath) {
    ifstream inFile(jsonFilePath);
    if (!inFile.is_open()) {
        cerr << "无法打开文件: " << jsonFilePath << endl;
        //output << "无法打开文件: " << jsonFilePath << endl;
        return {};
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();

    map<string, vector<Table>> databaseTables;
    for (const auto& tableJson : dbJson["tables"]) {
        Table table;
        table.tableName = tableJson["table_name"].get<string>();
        //table.databaseName = tableJson["database_name"].get<string>();
        table.databaseName = jsonFilePath.substr(0, jsonFilePath.size() - 4);
        table.fieldCount = tableJson.contains("field_count")
            ? tableJson["field_count"].get<int>()
            : tableJson["fields"].size();

        for (const auto& fieldJson : tableJson["fields"]) {
            Field field;
            field.name = fieldJson["name"].get<string>();
            field.type = fieldJson["type"].get<string>();
            field.keyFlag = fieldJson["key_flag"].get<string>();
            field.nullFlag = fieldJson["null_flag"].get<string>();
            field.validFlag = fieldJson["valid_flag"].get<string>();
            table.fields.push_back(field);
        }
        databaseTables[table.databaseName].push_back(table);
    }
    return databaseTables;
}
// 修剪字符串（去除前后空格）
string trim(const string& str) {
    size_t first = str.find_first_not_of(" \t");
    size_t last = str.find_last_not_of(" \t");
    return (first == string::npos || last == string::npos) ? "" : str.substr(first, last - first + 1);
}

// 错误处理
void throwError(const string& message) {
    cerr << "Error: " << message << endl;
    output<< "Error: " << message << endl;
    //writeResult();
    //exit(1);
}

// 解析字段定义
Field parseField(const string& fieldLine) {
    output << "parsing" << fieldLine << endl;
    regex fieldRegex(R"((\w+)\s+(\w+\[\d+\]|\w+)\s+(KEY|NOT_KEY)\s+(NO_NULL|NULL)\s+(VALID|NOT_VALID))",regex_constants::icase);
    regex fieldRegex2("(int|char|string)\\[\\d+\\]", regex_constants::icase);
    smatch match,match2;
    string trimmedField = trim(fieldLine);
    if (!regex_match(trimmedField, match, fieldRegex)) {
        throwError("Invalid field definition: " + fieldLine);
        return {"","","","",""};
    }
    if (match.size() <=2) {
        return { "","","","","" };
    }
    string trimmedField2 = trim(match[2]);
    output << "type is:" << trimmedField2<<endl;
    if (!regex_match(trimmedField2, match2, fieldRegex2)) {
        throwError("Invalid field definition: Must be int[],char[] or string[]");
        return {"","","","",""};
    }
    return { match[1], match[2], match[3], match[4], match[5] };
}


Field parseField(const string& fieldLine,string fromDatabaseName) {
    output << "parsing" << fieldLine << endl;
    regex fieldRegex(R"((\w+)\s+(\w+\[\d+\]|\w+)\s+(KEY|NOT_KEY)\s+(NO_NULL|NULL)\s+(VALID|NOT_VALID))", regex_constants::icase);
    regex fieldRegex2("(int|char|string)\\[\\d+\\]", regex_constants::icase);
    smatch match, match2;
    string trimmedField = trim(fieldLine);
    if (!regex_match(trimmedField, match, fieldRegex)) {
        throwError("Invalid field definition: " + fieldLine);
        return { "","","","","" };
    }
    if (match.size() <= 2) {
        return { "","","","","" };
    }
    string trimmedField2 = trim(match[2]);
    output << "type is:" << trimmedField2 << endl;
    if (!regex_match(trimmedField2, match2, fieldRegex2)) {
        throwError("Invalid field definition: Must be int[],char[] or string[]");
        return { "","","","","" };
    }
    bool has_key=false;
    for (auto it : databaseTables[fromDatabaseName]) {
        for (auto j : it.fields) {
            if (j.keyFlag == "KEY") {
                has_key = true;
                break;
            }
        }
    }
    if (has_key && match[3] == "KEY") {
        throwError("Invalid field definition: Already has KEY!");
        return { "","","","","" };
    }
    return { match[1], match[2], match[3], match[4], match[5] };
}
// 解析语句
Table parseCreateTableStatement(const string& sql) {
    Table table;
    regex mainRegex(R"(CREATE TABLE (\w+)\s*\((.+)\)\s*INTO (\w+);)", regex_constants::icase);
    smatch match;
    if (!regex_match(sql, match, mainRegex)) {
        throwError("Invalid CREATE TABLE statement.");
        return Table("","",0);
    }
    table.tableName = match[1];
    table.databaseName = match[3];
    string fieldsSection = match[2];
    istringstream fieldsStream(fieldsSection);
    string fieldLine;
    while (getline(fieldsStream, fieldLine, ',')) {
        Field field = parseField(fieldLine,table.databaseName);
        if (!(field.type == "" && field.name == "" && field.keyFlag == "" && field.nullFlag == "" && field.validFlag == ""))
            table.fields.push_back(field);
        else {
            cerr << "CREATE TABLE 参数错误：" << sql << endl;
            output << "CREATE TABLE 参数错误：" << sql << endl;
        }
    }
    table.fieldCount = table.fields.size();
    return table;
}

// 保存表信息到对应数据库的 JSON 文件
// 保存表信息到对应数据库的 JSON 文件
// 保存表信息到对应数据库的 JSON 文件//
void saveTablesToDatabaseFiles(map<string, vector<Table>>& databaseTables) {
    int cnt = -1;
    for (const auto& [databaseName, tables] : databaseTables) {
        cnt++;
        string filePath = databaseName + ".dbf";
        json dbJson;

        // 如果文件存在，读取现有数据
        ifstream inFile(filePath);
        if (inFile.is_open()) {
            try {
                inFile >> dbJson; // 解析已有文件
            }
            catch (const exception& e) {
                cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
                output << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
                inFile.close();
                continue;
            }
            inFile.close();
        }

        // 如果文件中没有 "tables" 字段，则初始化为空数组
        if (!dbJson.contains("tables")) {
            dbJson["tables"] = json::array();
        }

        // 提取已有表名集合
        set<string> existingTableNames;
        for (const auto& tableJson : dbJson["tables"]) {
            if (tableJson.contains("table_name")) {
                existingTableNames.insert(tableJson["table_name"]);
            }
        }

        // 检查和添加新表数据
        for ( auto table : tables) {
            // 检测新表中字段的 keyFlag 和类型
            int keyCount = 0;
            for (int i = 0; i < table.fields.size();i++) {   
                auto field = table.fields[i];
                if (field.keyFlag == "KEY") {
                    keyCount++;
                }

                // 检测字段类型是否合法
                const string& type = field.type;
                if (!regex_match(type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "表 " << table.tableName << " 中字段类型 " << type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    output << "表 " << table.tableName << " 中字段类型 " << type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    //table.fields.erase(table.fields.begin()+i);
                    //i--;
                    continue;
                    //return;
                }
            }

            if (keyCount > 1) {
                cerr << "表 " << table.tableName << " 中存在多个字段标记为 KEY，不允许。" << endl;
                output << "表 " << table.tableName << " 中存在多个字段标记为 KEY，不允许。" << endl;
                //table.fields.erase(table.fields.begin() + i);
                //i--;
                continue;
                //return;
            }

            if (existingTableNames.count(table.tableName)) {
                // 如果表已存在，更新字段数量
                for (auto& tableJson : dbJson["tables"]) {
                    if (tableJson["table_name"] == table.tableName) {
                        tableJson["field_count"] = table.fields.size(); // 更新字段数量
                    }
                }
            }
            else {
                // 如果表不存在，添加新表信息
                json newTable = table.toJson();
                newTable["field_count"] = table.fields.size(); // 增加字段数量
                dbJson["tables"].push_back(newTable);
            }

        }

        // 写回文件
        ofstream outFile(filePath);
        if (outFile.is_open()) {
            outFile << dbJson.dump(4); // 使用 4 个空格缩进
            outFile.close();
            cout << "表信息已保存到 " << filePath << endl;
            output << "表信息已保存到 " << filePath << endl;

        }
        else {
            cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
            output << "无法打开文件 " << filePath << " 进行写入。" << endl;
        }
        //if (cnt==databaseTables.size()-1)
        /*
        {
        filePath = databaseName + ".dbf";
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        }*/
    //create table TXX ( Age int[20] NOT_KEY NO_NULL VALID) into DB;
    //databaseTables = loadDatabaseTables(filePath);
    }
}
    

//new
bool isPrimaryKeyDuplicate(const string& databaseName, const string& tableName, size_t primaryKeyIndex, const string& primaryKeyValue) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    ifstream datFile(datFilePath, ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output << "表文件 " << datFilePath << " 不存在。" << endl;
        return false;
    }

    string line;
    while (getline(datFile, line)) {
        if (line.empty() || line[0] != '1') { // 跳过无效记录
            continue;
        }

        istringstream recordStream(line);
        vector<string> fields;
        string field;

        while (getline(recordStream, field, ',')) {
            fields.push_back(field);
        }

        if (primaryKeyIndex < fields.size() && fields[primaryKeyIndex] == primaryKeyValue) {
            return true; // 主键字段值已存在
        }
    }

    return false;
}

void consolidateToDatabaseFile(const string& databaseName, const vector<Table>& tables) {
    string databaseFilePath = databaseName + ".dat";
    ofstream databaseFile(databaseFilePath, ios::binary);

    if (!databaseFile.is_open()) {
        cerr << "无法创建主数据库文件: " << databaseFilePath << endl;
        output << "无法创建主数据库文件: " << databaseFilePath << endl;
        return;
    }

    databaseFile << "# " << databaseName << '\n'; // 写入数据库标识

    for (const auto& table : tables) {
        string tableFilePath = databaseName + "_" + table.tableName + ".dat";
        ifstream tableFile(tableFilePath, ios::binary);

        if (!tableFile.is_open()) {
            cerr << "无法打开表文件: " << tableFilePath << endl;
            output << "无法打开表文件: " << tableFilePath << endl;
            continue;
        }

        databaseFile << tableFile.rdbuf(); // 直接追加表文件内容到数据库文件
        databaseFile.put('\n');            // 表之间添加换行分隔
        tableFile.close();
    }

    databaseFile.close();
    cout << "所有表已成功汇总到数据库文件 " << databaseFilePath << " 中。" << endl;
    output << "所有表已成功汇总到数据库文件 " << databaseFilePath << " 中。" << endl;
}

// 编辑表字段//?汇总，数据随意改类型
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields, map<string, vector<Table>>& databaseTables) {
    string filePath = databaseName + ".dbf";
    string datFilePath = databaseName + "_" + tableName + ".dat";

    // 读取数据库文件
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output << "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    try {
        inFile >> dbJson;
    }
    catch (const exception& e) {
        cerr << "读取数据库文件时出错: " << e.what() << endl;
        output << "读取数据库文件时出错: " << e.what() << endl;
        inFile.close();
        return;
    }
    inFile.close();

    // 检查是否需要加载数据库到内存
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
            output << "加载数据库失败或数据库不存在：" << databaseName << endl;
            return;
        }
    }

    // 获取内存中的表对象
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "未找到表: " << tableName << endl;
        output << "未找到表: " << tableName << endl;
        return;
    }

    Table& table = *it;

    // 检查 .dat 文件是否有数据记录
    ifstream datFile(datFilePath);
    if (datFile.is_open()) {
        string line;
        for (int i = 0; i < 4; ++i) getline(datFile, line); // 跳过前 4 行
        if (getline(datFile, line)) {
            cerr << "表 " << tableName << " 有数据记录，无法修改字段类型。" << endl;
            output << "表 " << tableName << " 有数据记录，无法修改字段类型。" << endl;
            datFile.close();
            return;
        }
        datFile.close();
    }

    // 查找目标表
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            found = true;

            // 更新字段：逐一匹配并更新字段信息
            auto& existingFields = tableJson["fields"];
            for (const auto& newField : newFields) {
                bool fieldExists = false;
                if (!regex_match(newField.type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "字段类型 " << newField.type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    output << "字段类型 " << newField.type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    return;
                }
                for (auto& existingField : existingFields) {
                    if (existingField["name"] == newField.name) {
                        existingField["type"] = newField.type;
                        existingField["key_flag"] = newField.keyFlag;
                        existingField["null_flag"] = newField.nullFlag;
                        existingField["valid_flag"] = newField.validFlag;
                        fieldExists = true;
                        break;
                    }
                }

                if (!fieldExists) {
                    json newFieldJson = {
                        {"name", newField.name},
                        {"type", newField.type},
                        {"key_flag", newField.keyFlag},
                        {"null_flag", newField.nullFlag},
                        {"valid_flag", newField.validFlag}
                    };
                    existingFields.push_back(newFieldJson);
                }
            }

            // 确保主键唯一性
            if (std::any_of(newFields.begin(), newFields.end(), [](const Field& field) { return field.keyFlag == "KEY"; })) {
                for (auto& existingField : existingFields) {
                    if (std::none_of(newFields.begin(), newFields.end(), [&](const Field& field) { return field.name == existingField["name"]; })) {
                        existingField["key_flag"] = "NOT_KEY";
                    }
                }
            }

            break;
        }
    }

    if (!found) {
        cerr << "未找到表 " << tableName << "。" << endl;
        output << "未找到表 " << tableName << "。" << endl;
        return;
    }

    // 保存修改后的 JSON 数据
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4);
        outFile.close();
        cout << "表 " << tableName << " 的字段已更新。" << endl;
        output << "表 " << tableName << " 的字段已更新。" << endl;
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        output << "无法打开文件 " << filePath << " 进行写入。" << endl;
    }

    // 调用汇总函数
    tables = databaseTables[databaseName];
    consolidateToDatabaseFile(databaseName, tables);
}

// 更改表名
void renameTable(const string& oldTableName, const string& newTableName, const string& databaseName) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    json dbJson;

    // 读取现有数据
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // 解析已有 JSON 文件
        }
        catch (const exception& e) {
            cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            output<< "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << oldTableName << " 不存在。" << endl;
        output << "表 " << oldTableName << " 不存在。" << endl;
        return;
    }

    // 查找表并重命名
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == oldTableName) {
            tableJson["table_name"] = newTableName;
            found = true;
            break;
        }
    }

    if (!found) {
        cerr << "未找到表 " << oldTableName << "。" << endl;
        output<< "未找到表 " << oldTableName << "。" << endl;
        return;
    }

    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
        cout << "表 " << oldTableName << " 已重命名为 " << newTableName << "。\n";
        output<< "表 " << oldTableName << " 已重命名为 " << newTableName << "。\n";
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        output<< "无法打开文件 " << filePath << " 进行写入。" << endl;
    }
}

// 删除表//
void dropTable(const string& tableName, const string& databaseName) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    json dbJson;

    // 读取现有数据
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // 解析已有 JSON 文件
        }
        catch (const exception& e) {
            cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            output << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << tableName << " 不存在。" << endl;
        output << "表 " << tableName << " 不存在。" << endl;
        return;
    }
    // 检查并删除对应记录
    string datFile = databaseName + "_" + tableName + ".dat";

    if (fs::exists(datFile)) {
        output << "表" << tableName << "中已有记录一并删去。\n";
        fs::remove(datFile);
    }
    // 删除表
    auto& tables = dbJson["tables"];
    auto it = find_if(tables.begin(), tables.end(), [&](const json& tableJson) {
        return tableJson["table_name"] == tableName;
        });

    if (it != tables.end()) {
        tables.erase(it);
        cout << "表 " << tableName << " 已删除。\n";
        output << "表 " << tableName << " 已删除。\n";
    }
    else {
        cerr << "未找到表 " << tableName << "。\n";
        output << "未找到表 " << tableName << "。\n";
        return;
    }


    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        output << "无法打开文件 " << filePath << " 进行写入。" << endl;
    }
}

//显示模式
void readDatabaseFile(const string& databaseName) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output<< "无法打开数据库文件: " << filePath << endl;
        return;
    }

    try {
        json dbJson;
        inFile >> dbJson;
        inFile.close();

        if (!dbJson.contains("tables") || !dbJson["tables"].is_array()) {
            cerr << "数据库文件格式错误: 缺少 'tables' 字段。" << endl;
            output<< "数据库文件格式错误: 缺少 'tables' 字段。" << endl;
            return;
        }

        cout << "数据库: " << databaseName << endl;
        output<< "数据库: " << databaseName << endl;
        for (const auto& tableJson : dbJson["tables"]) {
            cout << "表名: " << tableJson["table_name"] << endl;
            output<< "表名: " << tableJson["table_name"] << endl;
            cout << "字段:" << endl;
            output<< "字段:" << endl;
            for (const auto& field : tableJson["fields"]) {
                cout << "  - 名称: " << field["name"]
                    << ", 类型: " << field["type"]
                    << ", 是否主键: " << field["key_flag"]
                    << ", 可为空: " << field["null_flag"]
                    << ", 是否有效: " << field["valid_flag"] << endl;
                output<< "  - 名称: " << field["name"]
                    << ", 类型: " << field["type"]
                    << ", 是否主键: " << field["key_flag"]
                    << ", 可为空: " << field["null_flag"]
                    << ", 是否有效: " << field["valid_flag"] << endl;
            }
        }
    }
    catch (const exception& e) {
        cerr << "解析数据库文件时发生错误: " << e.what() << endl;
        output<< "解析数据库文件时发生错误: " << e.what() << endl;
    }
}

//字段增加
void addFieldToTable(const string& tableName, const string& databaseName, const Field& newField) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output<< "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();

    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            for (const auto& field : tableJson["fields"]) {
                if (field["name"] == newField.name) {
                    cerr << "字段 " << newField.name << " 已存在于表 " << tableName << " 中。" << endl;
                    cout << "字段 " << newField.name << " 已存在于表 " << tableName << " 中。" << endl;
                    return;
                }
            }

            // 添加新字段
            json fieldJson;
            fieldJson["name"] = newField.name;
            fieldJson["type"] = newField.type;
            fieldJson["key_flag"] = newField.keyFlag;
            fieldJson["null_flag"] = newField.nullFlag;
            fieldJson["valid_flag"] = newField.validFlag;
            tableJson["fields"].push_back(fieldJson);

            // 更新字段数量
            tableJson["field_count"] = tableJson["fields"].size();

            ofstream outFile(filePath);
            if (outFile.is_open()) {
                outFile << dbJson.dump(4);
                outFile.close();
                cout << "字段 " << newField.name << " 添加成功，当前字段数量: " << tableJson["field_count"] << endl;
                output<< "字段 " << newField.name << " 添加成功，当前字段数量: " << tableJson["field_count"] << endl;
            }
            else {
                cerr << "无法更新数据库文件: " << filePath << endl;
                output<< "无法更新数据库文件: " << filePath << endl;
            }
            return;
        }
    }

    cerr << "未找到表: " << tableName << endl;
    output<< "未找到表: " << tableName << endl;
}


// 字段删除
void removeFieldFromTable(const string& tableName, const string& databaseName, const string& fieldName) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output<< "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();

    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            auto& fields = tableJson["fields"];
            auto it = std::remove_if(fields.begin(), fields.end(),
                [&fieldName](const json& field) { return field["name"] == fieldName; });

            if (it != fields.end()) {
                fields.erase(it, fields.end());

                // 更新字段数量
                tableJson["field_count"] = fields.size();

                // 写回数据库文件
                ofstream outFile(filePath);
                if (outFile.is_open()) {
                    outFile << dbJson.dump(4);
                    outFile.close();
                    cout << "字段 " << fieldName << " 删除成功，当前字段数量: " << tableJson["field_count"] << endl;
                    output<< "字段 " << fieldName << " 删除成功，当前字段数量: " << tableJson["field_count"] << endl;
                }
                else {
                    cerr << "无法更新数据库文件: " << filePath << endl;
                    output<< "无法更新数据库文件: " << filePath << endl;
                }
                return;
            }
            else {
                cerr << "未找到字段: " << fieldName << endl;
                output<< "未找到字段: " << fieldName << endl;
                return;
            }
        }
    }

    cerr << "未找到表: " << tableName << endl;
    output<< "未找到表: " << tableName << endl;
}



//修改字段
void modifyFieldInTable(const string& tableName, const string& databaseName, const string& oldFieldName, const string& newFieldName, const string& newType) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output<< "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            for (auto& field : tableJson["fields"]) {
                if (field["name"] == oldFieldName) {
                    // 根据输入参数进行修改
                    if (!newFieldName.empty()) {
                        field["name"] = newFieldName;
                    }
                    if (!newType.empty()) {
                        field["type"] = newType;
                    }

                    // 保存修改后的数据库文件
                    ofstream outFile(filePath);
                    if (outFile.is_open()) {
                        outFile << dbJson.dump(4);
                        //output << dbJson;
                        outFile.close();
                    }
                    cout << "字段 " << oldFieldName << " 已成功";
                    output<< "字段 " << oldFieldName << " 已成功";
                    if (!newFieldName.empty()) {
                        cout << " 修改为 " << newFieldName;
                        output<< " 修改为 " << newFieldName;
                    }
                    if (!newType.empty()) {
                        cout << "，类型修改为 " << newType;
                        output<< "，类型修改为 " << newType;
                    }
                    cout << endl;
                    output<< endl;
                    return;
                }
            }

            cerr << "未找到字段: " << oldFieldName << endl;
            output<< "未找到字段: " << oldFieldName << endl;
            return;
        }
    }

    cerr << "未找到表: " << tableName << endl;
    output<< "未找到表: " << tableName << endl;
}

//建表封装
void handleCreateTable(const string& sql, map<string, vector<Table>>& databaseTables) {
    Table table = parseCreateTableStatement(sql);
    if (!(table.fieldCount == 0)) {
        table.fieldCount = table.fields.size(); // 初始化字段数量
        databaseTables[table.databaseName].push_back(table);
        table.print();
        saveTablesToDatabaseFiles(databaseTables); // 保存表结构到文件
        cout << "表创建成功: " << table.tableName << "，字段数量: " << table.fieldCount << endl;
        output << "表创建成功: " << table.tableName << "，字段数量: " << table.fieldCount << endl;
    }
    else {
        cout << "表创建失败！ " << table.tableName << "字段数量: " << table.fieldCount << endl;
        output << "表创建失败！ " << table.tableName << "字段数量: " << table.fieldCount << endl;
    }
}

//表修改封装
void handleEditTable(const string& sql, map<string, vector<Table>>& databaseTables) {
    regex editRegex(R"(EDIT TABLE (\w+)\s*\(\s*(.+?)\s*\)\s*IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, editRegex)) {
        string tableName = match[1];
        string databaseName = match[3];
        string fieldsSection = match[2];

        vector<Field> newFields;
        istringstream fieldsStream(fieldsSection);
        string fieldLine;

        while (getline(fieldsStream, fieldLine, ',')) {
            Field field = parseField(fieldLine);//Field field = parseField(fieldLine);
            //
            if (!(field.type == "" && field.name == "" && field.keyFlag == "" && field.nullFlag == "" && field.validFlag == ""))
                newFields.push_back(field);
            else {
                cerr << "EDIT TABLE 参数错误：" << sql << endl;
                output << "EDIT TABLE 参数错误：" << sql << endl;
            }
        }

        editTableFields(tableName, databaseName, newFields,databaseTables);
    }
    else {
        cerr << "EDIT TABLE 语法错误：" << sql << endl;
        output<< "EDIT TABLE 语法错误：" << sql << endl;
    }
}

//表重命名封装
void handleRenameTable(const string& sql) {
    regex renameRegex(R"(RENAME TABLE (\w+)\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, renameRegex)) {
        string oldTableName = match[1];
        string newTableName = match[2];
        string databaseName = match[3];

        renameTable(oldTableName, newTableName, databaseName);
    }
    else {
        cerr << "RENAME TABLE 语法错误：" << sql << endl;
        output<< "RENAME TABLE 语法错误：" << sql << endl;
    }
}

//表删除封装
void handleDropTable(const string& sql) {
    regex dropRegex(R"(DROP TABLE (\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, dropRegex)) {
        string tableName = match[1];
        string databaseName = match[2];

        dropTable(tableName, databaseName);
    }
    else {
        cerr << "DROP TABLE 语法错误：" << sql << endl;
        output<< "DROP TABLE 语法错误：" << sql << endl;
    }
}

//读表
void handleReadDatabase(const string& sql) {
    regex readRegex(R"(READ DATABASE (\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, readRegex)) {
        string databaseName = match[1];
        readDatabaseFile(databaseName);
    }
    else {
        cerr << "READ DATABASE 语法错误：" << sql << endl;
        output<< "READ DATABASE 语法错误：" << sql << endl;
    }
}

//增加字段封装
void handleAddField(const string& sql) {
    regex addRegex(R"(ADD FIELD (\w+)\s*\((.+)\)\s*IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, addRegex)) {
        string tableName = match[1];
        string databaseName = match[3];
        Field newField = parseField(match[2],databaseName);//Field newField = parseField(match[2]);
        output << "new Field is:" << "@" <<newField.type << newField.name << newField.keyFlag<< newField.nullFlag<< newField.validFlag<< "@" << endl;
        if (!(newField.type == ""&& newField.name==""&&newField.keyFlag==""&&newField.nullFlag==""&&newField.validFlag==""))
            addFieldToTable(tableName, databaseName, newField);
        else {
            cerr << "ADD FIELD 参数错误：" << sql << endl;
            output << "ADD FIELD 参数错误：" << sql << endl;
        }
    }
    else {
        cerr << "ADD FIELD 语法错误：" << sql << endl;
        output<< "ADD FIELD 语法错误：" << sql << endl;
    }
}

//删除字段封装
void handleRemoveField(const string& sql) {
    regex removeRegex(R"(REMOVE FIELD (\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, removeRegex)) {
        string fieldName = match[1];
        string tableName = match[2];
        string databaseName = match[3];
        removeFieldFromTable(tableName, databaseName, fieldName);
    }
    else {
        cerr << "REMOVE FIELD 语法错误：" << sql << endl;
        output<< "REMOVE FIELD 语法错误：" << sql << endl;
    }
}

//修改字段名或类型封装
void handleModifyField(const string& sql) {
    // 正则表达式支持三种情况：
    // 1. 修改字段名和类型: MODIFY FIELD oldName TO newName TYPE newType IN tableName IN databaseName;
    // 2. 仅修改字段名: MODIFY FIELD oldName TO newName IN tableName IN databaseName;
    // 3. 仅修改字段类型: MODIFY FIELD oldName TYPE newType IN tableName IN databaseName;
    regex modifyRegexFull(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    regex modifyRegexNameOnly(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    regex modifyRegexTypeOnly(R"(MODIFY FIELD (\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);

    smatch match;

    if (regex_match(sql, match, modifyRegexFull)) {
        // 修改字段名和类型
        string oldFieldName = match[1];
        string newFieldName = match[2];
        string newType = match[3];
        string tableName = match[4];
        string databaseName = match[5];
        modifyFieldInTable(tableName, databaseName, oldFieldName, newFieldName, newType);
    }
    else if (regex_match(sql, match, modifyRegexNameOnly)) {
        // 仅修改字段名
        string oldFieldName = match[1];
        string newFieldName = match[2];
        string tableName = match[3];
        string databaseName = match[4];
        modifyFieldInTable(tableName, databaseName, oldFieldName, newFieldName, "");
    }
    else if (regex_match(sql, match, modifyRegexTypeOnly)) {
        // 仅修改字段类型
        string oldFieldName = match[1];
        string newType = match[2];
        string tableName = match[3];
        string databaseName = match[4];
        modifyFieldInTable(tableName, databaseName, oldFieldName, "", newType);
    }
    else {
        cerr << "MODIFY FIELD 语法错误：" << sql << endl;
        output<< "MODIFY FIELD 语法错误：" << sql << endl;
    }
}



void createTableDatFile(const string& databaseName, const Table& table) {
    string datFilePath = databaseName + "_" + table.tableName + ".dat";
    ofstream datFile(datFilePath, ios::binary);

    if (!datFile.is_open()) {
        cerr << "无法创建表文件: " << datFilePath << endl;
        output<< "无法创建表文件: " << datFilePath << endl;
        return;
    }

    datFile << "~" << table.tableName << '\n'; // 写入表名
    datFile << "0\n";                          // 初始化记录数量为 0
    datFile << table.fieldCount << '\n';       // 写入字段数量

    datFile << "Valid" << ","; // "Valid" 对齐
    // 写入字段名并对齐
    for (const auto& field : table.fields) {
        datFile << field.name << ",";
    }

    datFile << '\n';
    datFile.close();
    cout << "表 " << table.tableName << " 的 .dat 文件创建成功。" << endl;
    output<< "表 " << table.tableName << " 的 .dat 文件创建成功。" << endl;
}

void insertRecordToTableFile(const string& databaseName, const string& tableName, const vector<string>& values) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output<< "表文件 " << datFilePath << " 不存在。" << endl;
        return;
    }

    datFile.seekg(0, ios::beg);
    string line;
    int recordCount = 0;

    // 读取当前记录数量
    while (getline(datFile, line)) {
        if (line.find_first_not_of("0123456789") == string::npos) { // 检测是否为记录数量行
            recordCount = stoi(line);
            recordCount++;
            break;
        }
    }

    // 更新记录数量
    datFile.clear();
    datFile.seekp(0, ios::beg);
    while (getline(datFile, line)) {
        if (line.find_first_not_of("0123456789") == string::npos) {
            streampos currentPos = datFile.tellg();
            streamoff offset = static_cast<streamoff>(line.size()) + 1;
            datFile.seekp(currentPos - offset);
            datFile << recordCount << '\n';
            break;
        }
    }

    // 定位到文件末尾写入新记录
    datFile.clear();
    datFile.seekp(0, ios::end);
    datFile << "1,"; // 有效标志和换行符
    for (size_t i = 0; i < values.size(); ++i) {
        datFile << left << values[i] << ",";

    }
    datFile << '\n';

    datFile.close();
    cout << "记录插入到表 " << tableName << " 成功。" << endl;
    output<< "记录插入到表 " << tableName << " 成功。" << endl;
}


void insertRecord(const string& databaseName, const string& tableName, const vector<string>& values, map<string, vector<Table>>& databaseTables) {
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cerr << "数据库 " << databaseName << " 不存在。" << endl;
        output << "数据库 " << databaseName << " 不存在。" << endl;
        return;
    }

    auto& tables = databaseTables[databaseName];
    auto it = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "表 " << tableName << " 不存在。" << endl;
        output << "表 " << tableName << " 不存在。" << endl;
        return;
    }

    Table& table = *it;

    if (values.size() != table.fieldCount) {
        cerr << "插入的字段数量不匹配，表 " << tableName << " 需要 " << table.fieldCount << " 个字段。" << endl;
        output << "插入的字段数量不匹配，表 " << tableName << " 需要 " << table.fieldCount << " 个字段。" << endl;
        return;
    }

    // 验证主键字段是否重复
    for (size_t i = 0; i < table.fields.size(); ++i) {
        const auto& field = table.fields[i];
        if (field.keyFlag == "KEY") {
            if (isPrimaryKeyDuplicate(databaseName, tableName, i + 1, values[i])) {
                cerr << "字段 " << field.name << " 的值 \"" << values[i] << "\" 已存在，违反主键约束。" << endl;
                output << "字段 " << field.name << " 的值 \"" << values[i] << "\" 已存在，违反主键约束。" << endl;
                return;
            }
        }
    }

    // 验证字段类型和长度
    for (size_t i = 0; i < table.fields.size(); ++i) {
        const auto& field = table.fields[i];
        const auto& value = values[i];

        // 提取字段类型和长度
        regex typeRegex(R"((int|char|string)\[(\d+)\])", regex_constants::icase);
        smatch match;

        if (regex_match(field.type, match, typeRegex)) {
            string type = match[1];
            int length = stoi(match[2]);

            // 验证字段长度
            if (value.length() > length) {
                cerr << "字段 " << field.name << " 的值 \"" << value << "\" 长度超出限制 (" << length << ")。" << endl;
                output << "字段 " << field.name << " 的值 \"" << value << "\" 长度超出限制 (" << length << ")。" << endl;
                return;
            }

            // 验证字段类型
            if (type == "int") {
                if (!regex_match(value, regex(R"(\d+)"))) {
                    cerr << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 int 类型要求。" << endl;
                    output << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 int 类型要求。" << endl;
                    return;
                }
            }
            else if (type == "char") {
                if (!regex_match(value, regex(R"([a-zA-Z]+)"))) {
                    cerr << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 char 类型要求。" << endl;
                    output << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 char 类型要求。" << endl;
                    return;
                }
            }
            // string 类型无需额外验证
        }
        else {
            cerr << "字段 " << field.name << " 的类型 \"" << field.type << "\" 无效。" << endl;
            output << "字段 " << field.name << " 的类型 \"" << field.type << "\" 无效。" << endl;
            return;
        }
    }

    // 如果表文件不存在，则创建新表文件
    string tableFilePath = databaseName + "_" + tableName + ".dat";
    ifstream checkFile(tableFilePath, ios::binary);
    if (!checkFile.is_open()) {
        createTableDatFile(databaseName, table);
    }
    checkFile.close();

    // 插入记录到表文件
    insertRecordToTableFile(databaseName, tableName, values);
    consolidateToDatabaseFile(databaseName, tables);
    cout << "记录已插入表 " << tableName << "，并更新至数据库文件 " << databaseName << endl;
    output << "记录已插入表 " << tableName << "，并更新至数据库文件 " << databaseName << endl;
}

void deleteRecordFromTableFile(const string& databaseName, const string& tableName, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output << "表文件 " << datFilePath << " 不存在。" << endl;
        return;
    }

    datFile.seekg(0, ios::beg);
    string line;
    vector<string> fieldNames;
    map<string, int> fieldIndexMap; // 字段名到索引的映射
    int fieldCount = 0;
    int recordCount = 0;
    int sum1 = 0;
    // 跳过表名和记录数量行
    getline(datFile, line); // 表名行
    getline(datFile, line); // 记录数量行
    recordCount = stoi(line);
    getline(datFile, line); // 字段数量行
    fieldCount = stoi(line);

    // 解析字段名行
    if (getline(datFile, line)) {
        stringstream ss(line);
        string field;
        int index = 0;
        while (getline(ss, field, ',')) {
            field = trim(field); // 去掉多余空格
            fieldNames.push_back(field);
            fieldIndexMap[field] = index++;
        }
    }

    // 检查条件字段是否存在
    for (const auto& condition : conditions) {
        if (fieldIndexMap.find(condition.first) == fieldIndexMap.end()) {
            cerr << "字段 " << condition.first << " 不存在。" << endl;
            output << "字段 " << condition.first << " 不存在。" << endl;
            datFile.close();
            return;
        }
    }

    bool found = false;
    vector<string> updatedLines;
    int sum = 0;
    // 遍历记录，逐行扫描
    while (getline(datFile, line)) {
        stringstream ss(line);
        vector<string> recordFields;
        string fieldValue;

        // 按逗号分隔解析记录
        while (getline(ss, fieldValue, ',')) {
            recordFields.push_back(trim(fieldValue)); // 去掉多余空格
        }

        // 检查是否满足所有条件
        bool matches = true;
        for (const auto& condition : conditions) {
            int index = fieldIndexMap[condition.first];
            if (recordFields[index] != condition.second) {
                matches = false;
                break;
            }
        }

        // 如果满足条件且记录有效，则标记为无效
        if (matches && recordFields[0] == "1") {
            found = true;
            recordFields[0] = "0"; // 将有效位设为无效
            sum1 = sum1 + 1;
        }

        // 重组更新的记录行
        string updatedLine;
        for (size_t i = 0; i < recordFields.size(); ++i) {
            updatedLine += recordFields[i];
            if (i < recordFields.size() - 1) {
                updatedLine += ",";
            }
        }
        updatedLines.push_back(updatedLine);
    }

    datFile.close();

    if (found) {
        // 写回文件
        ofstream outFile(datFilePath, ios::out | ios::trunc | ios::binary);
        if (!outFile.is_open()) {
            cerr << "无法打开文件进行写回操作。" << endl;
            output << "无法打开文件进行写回操作。" << endl;
            return;
        }

        // 写回表名、记录数量和字段名行
        outFile << "~" << tableName << '\n';
        outFile << recordCount - sum1 << '\n'; // 更新记录数量
        outFile << fieldCount << '\n';      // 写回字段数量
        for (const auto& field : fieldNames) {
            outFile << field << ",";
        }
        outFile << '\n';

        // 写回更新后的记录
        for (const auto& updatedLine : updatedLines) {
            outFile << updatedLine << "," << '\n';
        }
        outFile.close();

        // 汇总到数据库文件
        auto& tables = databaseTables[databaseName];
        consolidateToDatabaseFile(databaseName, tables);
        cout << "记录删除成功（标记为无效）。" << endl;
        output << "记录删除成功（标记为无效）。" << endl;
    }
    else {
        cerr << "未找到满足条件的记录，或记录已无效。" << endl;
        output << "未找到满足条件的记录，或记录已无效。" << endl;
    }
}

int validateFieldValue(const Field& field, const string& value) {
    // 提取字段类型和长度
    regex typeRegex(R"((int|char|string)\[(\d+)\])", regex_constants::icase);
    smatch match;
    if (regex_match(field.type, match, typeRegex)) {
        string type = match[1];  // 提取字段类型
        int length = stoi(match[2]);  // 提取字段长度

        // 验证字段长度
        if (value.length() > length) {
            cerr << "字段 " << field.name << " 的值 \"" << value << "\" 长度超出限制 (" << length << ")。" << endl;
            output << "字段 " << field.name << " 的值 \"" << value << "\" 长度超出限制 (" << length << ")。" << endl;
            return false;
        }

        // 验证字段类型
        if (type == "int") {
            if (!regex_match(value, regex(R"(\d+)"))) {
                cerr << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 int 类型要求。" << endl;
                output << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 int 类型要求。" << endl;
                return false;
            }
        }
        else if (type == "char") {
            if (!regex_match(value, regex(R"([a-zA-Z]+)"))) {
                cerr << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 char 类型要求。" << endl;
                output << "字段 " << field.name << " 的值 \"" << value << "\" 不符合 char 类型要求。" << endl;
                return false;
            }
        }
        // string 类型无需额外验证
    }
    else {
        cerr << "字段类型定义 \"" << field.type << "\" 无法解析。" << endl;
        output << "字段类型定义 \"" << field.type << "\" 无法解析。" << endl;
        return false;
    }
    return true;
}


void updateRecordInTableFile(const string& databaseName, const string& tableName, const string& targetField, const string& newValue, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output << "表文件 " << datFilePath << " 不存在。" << endl;
        return;
    }

    datFile.seekg(0, ios::beg);
    string line;
    vector<string> fieldNames;
    map<string, int> fieldIndexMap; // 字段名到索引的映射
    int fieldCount = 0;
    int recordCount = 0;

    // 跳过表名和记录数量行
    getline(datFile, line); // 表名行
    getline(datFile, line); // 记录数量行
    recordCount = stoi(line);
    getline(datFile, line); // 字段数量行
    fieldCount = stoi(line);

    // 解析字段名和类型行
    if (getline(datFile, line)) {
        stringstream ss(line);
        string field;
        int index = 0;
        while (getline(ss, field, ',')) {
            size_t pos = field.find(':');
            string fieldName = trim(field.substr(0, pos));
            fieldNames.push_back(fieldName);
            fieldIndexMap[fieldName] = index++;
        }
    }

    // 确保表存在于数据库中
    auto it = databaseTables.find(databaseName);
    if (it == databaseTables.end()) {
        cerr << "数据库 " << databaseName << " 不存在。" << endl;
        return;
    }

    auto& tables = it->second;
    auto tableIt = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (tableIt == tables.end()) {
        cerr << "表 " << tableName << " 不存在于数据库 " << databaseName << " 中。" << endl;
        return;
    }

    Table& table = *tableIt;

    // 检查目标字段是否为主键
    bool isPrimaryKey = false;
    size_t primaryKeyIndex = 0; // 主键索引
    for (const auto& field : table.fields) {
        if (field.name == targetField) {
            if (field.keyFlag == "KEY") {
                isPrimaryKey = true;
            }
            primaryKeyIndex = fieldIndexMap[targetField];
            break;
        }
    }

    // 如果目标字段是主键，检查新值是否重复
    if (isPrimaryKey && isPrimaryKeyDuplicate(databaseName, tableName, primaryKeyIndex, newValue)) {
        cerr << "更新失败：主键字段 " << targetField << " 的值 \"" << newValue << "\" 已存在。" << endl;
        output << "更新失败：主键字段 " << targetField << " 的值 \"" << newValue << "\" 已存在。" << endl;
        datFile.close();
        return;
    }

    // 更新逻辑保持不变
    bool updated = false;
    vector<string> updatedLines;

    // 遍历记录，逐行扫描
    while (getline(datFile, line)) {
        stringstream ss(line);
        vector<string> recordFields;
        string fieldValue;

        // 按逗号分隔解析记录
        while (getline(ss, fieldValue, ',')) {
            recordFields.push_back(trim(fieldValue)); // 去掉多余空格
        }

        // 检查是否满足所有条件
        bool matches = true;
        for (const auto& condition : conditions) {
            int index = fieldIndexMap[condition.first];
            if (recordFields[index] != condition.second) {
                matches = false;
                break;
            }
        }

        // 如果满足条件且记录有效，则更新目标字段
        if (matches && recordFields[0] == "1") {
            int targetIndex = fieldIndexMap[targetField];
            recordFields[targetIndex] = newValue; // 更新字段值
            updated = true;
        }

        // 重组更新的记录行
        string updatedLine;
        for (size_t i = 0; i < recordFields.size(); ++i) {
            updatedLine += recordFields[i];
            if (i < recordFields.size() - 1) {
                updatedLine += ",";
            }
        }
        updatedLines.push_back(updatedLine);
    }

    datFile.close();

    if (updated) {
        // 写回文件
        ofstream outFile(datFilePath, ios::out | ios::trunc | ios::binary);
        if (!outFile.is_open()) {
            cerr << "无法打开文件进行写回操作。" << endl;
            output << "无法打开文件进行写回操作。" << endl;
            return;
        }

        // 写回表名、记录数量和字段名行
        outFile << "~" << tableName << '\n';
        outFile << recordCount << '\n';  // 记录数量保持不变
        outFile << fieldCount << '\n';  // 字段数量保持不变
        for (const auto& field : fieldNames) {
            outFile << field << ",";
        }
        outFile << '\n';

        // 写回更新后的记录
        for (const auto& updatedLine : updatedLines) {
            outFile << updatedLine << "," << '\n';
        }
        outFile.close();

        // 汇总到数据库文件
        auto& tables = databaseTables[databaseName];
        consolidateToDatabaseFile(databaseName, tables);
        cout << "记录更新成功。" << endl;
        output << "记录更新成功。" << endl;
    }
    else {
        cerr << "未找到满足条件的记录，或记录无效。" << endl;
        output << "未找到满足条件的记录，或记录无效。" << endl;
    }
}

//表重命名//
void renameTable(const string& oldTableName, const string& newTableName, const string& databaseName, map<string, vector<Table>>& databaseTables) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    json dbJson;

    // 读取现有数据
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // 解析已有 JSON 文件
        }
        catch (const exception& e) {
            cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            output << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << oldTableName << " 不存在。" << endl;
        output << "表 " << oldTableName << " 不存在。" << endl;
        return;
    }

    // 查找表并重命名
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == oldTableName) {
            tableJson["table_name"] = newTableName;
            found = true;
            break;
        }
    }

    if (!found) {
        cerr << "未找到表 " << oldTableName << "。" << endl;
        output << "未找到表 " << oldTableName << "。" << endl;
        return;
    }

    // 检查并重命名对应的 .dat 文件
    string oldDatFile = databaseName + "_" + oldTableName + ".dat";
    string newDatFile = databaseName + "_" + newTableName + ".dat";

    if (fs::exists(oldDatFile)) {
        try {
            fs::rename(oldDatFile, newDatFile);
            cout << "表记录文件已从 " << oldDatFile << " 重命名为 " << newDatFile << "。\n";
            output << "表记录文件已从 " << oldDatFile << " 重命名为 " << newDatFile << "。\n";
        }
        catch (const fs::filesystem_error& e) {
            cerr << "重命名表记录文件时发生错误：" << e.what() << endl;
            output << "重命名表记录文件时发生错误：" << e.what() << endl;
            return;
        }
    }
    else {
        cout << "未找到表记录文件 " << oldDatFile << "，可能表无对应数据文件。\n";
        output << "未找到表记录文件 " << oldDatFile << "，可能表无对应数据文件。\n";
    }

    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
        cout << "表 " << oldTableName << " 已重命名为 " << newTableName << "。\n";
        output << "表 " << oldTableName << " 已重命名为 " << newTableName << "。\n";
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        output << "无法打开文件 " << filePath << " 进行写入。" << endl;
        return;
    }

    // 检查是否需要加载数据库
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf"); // 假设 JSON 文件路径为 "MyDB.dbf"
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
            output << "加载数据库失败或数据库不存在：" << databaseName << endl;
            return;
        }
    }
    // 更新内存中表名
    if (databaseTables.find(databaseName) != databaseTables.end()) {
        for (auto& table : databaseTables[databaseName]) {
            if (table.tableName == oldTableName) {
                table.tableName = newTableName;
                break;
            }
        }
    }


    fstream datFile(newDatFile, ios::in | ios::out);
    if (datFile.is_open()) {
        // 覆盖写入第一行
        datFile.seekp(0, ios::beg);
        datFile << "~" << newTableName << '\n';
        datFile.close();
        cout << "新表名 " << newTableName << " 已覆盖写入表记录文件 " << newDatFile << " 的第一行。\n";
        output << "新表名 " << newTableName << " 已覆盖写入表记录文件 " << newDatFile << " 的第一行。\n";
    }
    else {
        cerr << "无法打开表记录文件 " << newDatFile << " 进行写入。" << endl;
        output << "无法打开表记录文件 " << newDatFile << " 进行写入。" << endl;
    }

    // 调用汇总函数
    auto& tables = databaseTables[databaseName];
    consolidateToDatabaseFile(databaseName, tables);
}

//表重命名封装//
void handleRenameTable(const string& sql, map<string, vector<Table>>& databaseTables) {
    regex renameRegex(R"(RENAME TABLE (\w+)\s+(\w+)\s+IN\s+(\w+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, renameRegex)) {
        string oldTableName = match[1];
        string newTableName = match[2];
        string databaseName = match[3];

        renameTable(oldTableName, newTableName, databaseName, databaseTables);
    }
    else {
        cerr << "RENAME TABLE 语法错误：" << sql << endl;
        output << "RENAME TABLE 语法错误：" << sql << endl;
    }
}

//字段增加//
void addFieldToTable(const string& tableName, const string& databaseName, const Field& newField, map<string, vector<Table>>& databaseTables) {
    // 检查是否需要加载数据库到内存
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
            output << "加载数据库失败或数据库不存在：" << databaseName << endl;
            return;
        }
    }

    // 获取内存中的表对象
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "未找到表: " << tableName << endl;
        output << "未找到表: " << tableName << endl;
        return;
    }

    Table& table = *it;

    // 检查字段是否已存在
    for (const auto& field : table.fields) {
        if (field.name == newField.name) {
            cerr << "字段 " << newField.name << " 已存在于表 " << tableName << " 中。" << endl;
            output << "字段 " << newField.name << " 已存在于表 " << tableName << " 中。" << endl;
            return;
        }
    }
    // 检测字段类型是否合法
    if (!regex_match(newField.type, regex("(int|char|string)\\[\\d+\\]"))) {
        cerr << "表 " << table.tableName << " 中字段类型 " << newField.type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
        output << "表 " << table.tableName << " 中字段类型 " << newField.type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
        return;
    }
    // 检查是否已有主键字段，且新字段是主键
    if (newField.keyFlag == "KEY") {
        for (const auto& field : table.fields) {
            if (field.keyFlag == "KEY") {
                cerr << "表中已有主键字段，无法添加新主键字段 " << newField.name << endl;
                output << "表中已有主键字段，无法添加新主键字段 " << newField.name << endl;
                return;
            }
        }
    }
    int op = 0;
    string datFilePath = databaseName + "_" + table.tableName + ".dat";
    fstream datFile(datFilePath, ios::in);
    // 检查是否已有非空字段，且新字段是非空
    if (newField.nullFlag == "NO_NULL") {
        if (datFile.is_open()) {
            cerr << "表文件 " << datFilePath << " 存在,则不允许非空字段添加" << endl;
            output << "表文件 " << datFilePath << " 存在,则不允许非空字段添加" << endl;
            return;
        }
        else {
            op = -1;
        }

        // 读取文件内容以确认现有数据是否满足非空要求
        vector<string> fileLines;
        string line;
        while (getline(datFile, line)) {
            fileLines.push_back(line);
        }

        // 仅从第五行开始才有数据
        if (fileLines.size() > 4) {
            for (size_t i = 4; i < fileLines.size(); ++i) {
                stringstream ss(fileLines[i]);
                string cell;
                int fieldCountInLine = 0;

                // 计算此行的字段数
                while (getline(ss, cell, ',')) {
                    fieldCountInLine++;
                }
                // 如果新字段为非空，则报错
                if (newField.nullFlag == "NO_NULL") {
                    cerr << "已有数据，且新字段为非空字段，无法添加字段 " << newField.name << endl;
                    output << "已有数据，且新字段为非空字段，无法添加字段 " << newField.name << endl;
                    datFile.close();
                    return;
                }
            }
        }
        datFile.close();
    }

    // 添加新字段到内存
    table.fields.push_back(newField);
    table.fieldCount = table.fields.size();

    // 更新到数据库文件
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output << "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();

    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            json fieldJson;
            fieldJson["name"] = newField.name;
            fieldJson["type"] = newField.type;
            fieldJson["key_flag"] = newField.keyFlag;
            fieldJson["null_flag"] = newField.nullFlag;
            fieldJson["valid_flag"] = newField.validFlag;
            tableJson["fields"].push_back(fieldJson);

            tableJson["field_count"] = table.fieldCount; // 更新字段数量
            break;
        }
    }

    // 写回更新后的 JSON 文件
    ofstream outFile1(filePath);
    if (outFile1.is_open()) {
        outFile1 << dbJson.dump(4);
        outFile1.close();
        cout << "字段 " << newField.name << " 添加成功，当前字段数量: " << table.fieldCount << endl;
        output << "字段 " << newField.name << " 添加成功，当前字段数量: " << table.fieldCount << endl;
    }
    else {
        cerr << "无法更新数据库文件: " << filePath << endl;
        output << "无法更新数据库文件: " << filePath << endl;
    }

    fstream datFile1(datFilePath, ios::in | ios::out);
    if (!datFile1.is_open()) {
        cerr << "无法打开表文件: " << datFilePath << endl;
        output << "无法打开表文件: " << datFilePath << endl;
        return;
    }

    // 读取文件的所有内容到内存
    vector<string> fileLines;
    string line;
    while (getline(datFile1, line)) {
        fileLines.push_back(line);
    }

    // 确保文件包含至少四行
    if (fileLines.size() < 4) {
        cerr << "文件内容少于 4 行，无法修改第三和第四行。" << endl;
        output << "文件内容少于 4 行，无法修改第三和第四行。" << endl;
        datFile1.close();
        return;
    }

    // 修改第三行：更新字段数
    int fieldCount = table.fields.size(); // 当前字段总数
    fileLines[2] = to_string(fieldCount); // 第三行为字段数

    // 修改第四行：更新字段名称
    string newLine = "Valid,";
    for (const auto& field : table.fields) {
        newLine += field.name + ",";
    }
    fileLines[3] = newLine; // 第四行为字段名称

    // 补全已有数据行中的字段（如果数据不足，则补NULL）
    for (size_t i = 4; i < fileLines.size(); ++i) {
        stringstream ss(fileLines[i]);
        string cell;
        vector<string> row;
        while (getline(ss, cell, ',')) {
            row.push_back(cell);
        }

        // 如果数据列数小于新的字段数，则补充NULL
        while (row.size() < table.fields.size() + 1) {
            row.push_back("NULL");
        }

        // 重新构造这一行
        string updatedLine = "";
        for (size_t j = 0; j < row.size(); ++j) {
            updatedLine += row[j] + ",";
        }
        //updatedLine.pop_back();  // 去除最后一个多余的逗号
        fileLines[i] = updatedLine;
    }

    // 将修改后的内容写回文件
    datFile.close(); // 关闭文件，准备重新写入
    ofstream outFile(datFilePath, ios::trunc | ios::binary);
    if (!outFile.is_open()) {
        cerr << "无法重新写入表文件: " << datFilePath << endl;
        output << "无法重新写入表文件: " << datFilePath << endl;
        return;
    }

    for (const auto& updatedLine : fileLines) {
        outFile << updatedLine << '\n';
    }
    outFile.close();

    cout << "第三行和第四行修改完成，字段数：" << fieldCount << ", 字段名称已更新。" << endl;
    output << "第三行和第四行修改完成，字段数：" << fieldCount << ", 字段名称已更新。" << endl;

    // 调用汇总函数
    consolidateToDatabaseFile(databaseName, tables);
}


//增加字段封装//
void handleAddField(const string& sql, map<string, vector<Table>>& databaseTables) {
    regex addRegex(R"(ADD FIELD (\w+)\s*\((.+)\)\s*IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, addRegex)) {
        string tableName = match[1];
        string databaseName = match[3];
        Field newField = parseField(match[2],databaseName);//Field newField = parseField(match[2]);
        if (!(newField.type == "" && newField.name == "" && newField.keyFlag == "" && newField.nullFlag == "" && newField.validFlag == ""))
            addFieldToTable(tableName, databaseName, newField, databaseTables);
        else {
            cerr << "ADD FIELD 参数错误：" << sql << endl;
            output << "ADD FIELD 参数错误：" << sql << endl;
        }
    }
    else {
        cerr << "ADD FIELD 语法错误：" << sql << endl;
        output << "ADD FIELD 语法错误：" << sql << endl;
    }
}

// 字段删除////
void removeFieldFromTable(const string& tableName, const string& databaseName, const string& fieldName, map<string, vector<Table>>& databaseTables) {
    // 确保数据库已加载
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
            output << "加载数据库失败或数据库不存在：" << databaseName << endl;
            return;
        }
    }

    // 获取表对象
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "未找到表: " << tableName << endl;
        output << "未找到表: " << tableName << endl;
        return;
    }

    Table& table = *it;
    auto fieldIt = std::find_if(table.fields.begin(), table.fields.end(), [&](const Field& field) {
        return field.name == fieldName;
        });

    if (fieldIt == table.fields.end()) {
        cerr << "未找到字段: " << fieldName << endl;
        output << "未找到字段: " << fieldName << endl;
        return;
    }

    // 找到字段索引
    int fieldIndex = std::distance(table.fields.begin(), fieldIt);

    // 删除字段元信息
    table.fields.erase(fieldIt);
    table.fieldCount = table.fields.size();

    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in);
    if (datFile.is_open()) {

        // 读取文件内容
        vector<string> fileLines;
        string line;
        while (getline(datFile, line)) {
            fileLines.push_back(line);
        }
        datFile.close();

        // 修改第三行字段数
        if (fileLines.size() < 4) {
            cerr << "文件内容少于 4 行，无法完成操作。" << endl;
            output << "文件内容少于 4 行，无法完成操作。" << endl;
            return;
        }
        fileLines[2] = to_string(table.fieldCount);

        // 修改第四行字段名称
        string newHeader = "Valid,";
        for (const auto& field : table.fields) {
            newHeader += field.name + ",";
        }
        fileLines[3] = newHeader;

        // 删除数据行中对应字段
        for (size_t i = 4; i < fileLines.size(); ++i) {
            stringstream ss(fileLines[i]);
            vector<string> cells;
            string cell;
            while (getline(ss, cell, ',')) {
                cells.push_back(cell);
            }

            // 删除目标字段位置
            if (fieldIndex + 1 < cells.size()) {
                cells.erase(cells.begin() + fieldIndex + 1);
            }

            // 重新构造行
            string updatedLine;
            for (const auto& c : cells) {
                updatedLine += c + ",";
            }
            fileLines[i] = updatedLine;
        }

        // 写回修改后的文件
        ofstream outFile(datFilePath, ios::trunc | ios::binary);
        if (!outFile.is_open()) {
            cerr << "无法重新写入表文件: " << datFilePath << endl;
            output << "无法重新写入表文件: " << datFilePath << endl;
            return;
        }
        for (const auto& updatedLine : fileLines) {
            outFile << updatedLine << '\n';
        }
        outFile.close();
    }
    // 更新数据库元信息文件
    string dbfFilePath = databaseName + ".dbf";
    ifstream dbfFile(dbfFilePath);
    if (!dbfFile.is_open()) {
        cerr << "无法打开数据库元信息文件: " << dbfFilePath << endl;
        output << "无法打开数据库元信息文件: " << dbfFilePath << endl;
        return;
    }

    json dbJson;
    dbfFile >> dbJson;
    dbfFile.close();

    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            auto& fields = tableJson["fields"];
            auto it = std::remove_if(fields.begin(), fields.end(), [&](const json& field) {
                return field["name"] == fieldName;
                });
            fields.erase(it, fields.end());
            tableJson["field_count"] = table.fieldCount;
            break;
        }
    }

    ofstream dbfOutFile(dbfFilePath);
    if (dbfOutFile.is_open()) {
        dbfOutFile << dbJson.dump(4);
        dbfOutFile.close();
        cout << "字段 " << fieldName << " 删除成功，当前字段数: " << table.fieldCount << endl;
        output << "字段 " << fieldName << " 删除成功，当前字段数: " << table.fieldCount << endl;
    }
    // 调用汇总函数
    consolidateToDatabaseFile(databaseName, tables);
}

//删除字段封装//
void handleRemoveField(const string& sql, map<string, vector<Table>>& databaseTables) {
    regex removeRegex(R"(REMOVE FIELD (\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, removeRegex)) {
        string fieldName = match[1];
        string tableName = match[2];
        string databaseName = match[3];
        removeFieldFromTable(tableName, databaseName, fieldName, databaseTables);
    }
    else {
        cerr << "REMOVE FIELD 语法错误：" << sql << endl;
        output << "REMOVE FIELD 语法错误：" << sql << endl;
    }
}

//修改字段//?数据随意改类型//
void modifyFieldInTable(const string& tableName, const string& databaseName, const string& oldFieldName, const string& newFieldName, const string& newType, map<string, vector<Table>>& databaseTables) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        output << "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    try {
        inFile >> dbJson;
    }
    catch (const exception& e) {
        cerr << "读取数据库文件时出错: " << e.what() << endl;
        output << "读取数据库文件时出错: " << e.what() << endl;
        inFile.close();
        return;
    }
    inFile.close();
    // 检查是否需要加载数据库到内存
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
            output << "加载数据库失败或数据库不存在：" << databaseName << endl;
            return;
        }
    }

    // 获取内存中的表对象
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "未找到表: " << tableName << endl;
        output << "未找到表: " << tableName << endl;
        return;
    }

    Table& table = *it;
    // 查找目标表
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            // 检查字段值是否重复
            for (const auto& field : tableJson["fields"]) {
                if (field["name"] == newFieldName && field["name"] != oldFieldName) {
                    cerr << "表 " << tableName << " 中已存在字段名称: " << newFieldName << endl;
                    output << "表 " << tableName << " 中已存在字段名称: " << newFieldName << endl;
                    return;
                }
            }

            // 获取 `dat` 文件路径
            string datFilePath = databaseName + "_" + tableName + ".dat";
            fstream datFile(datFilePath, ios::in | ios::out);
            bool datFileExists = datFile.is_open();

            // 查找目标字段并修改
            bool fieldFound = false; // 标志位
            for (auto& field : tableJson["fields"]) {
                if (field["name"] == oldFieldName) {
                    fieldFound = true;

                    // 检查新字段类型是否有效
                    if (!newType.empty() && !regex_match(newType, regex("(int|char|string)\\[\\d+\\]"))) {
                        cerr << "表 " << tableName << " 中字段类型 " << newType << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                        output << "表 " << tableName << " 中字段类型 " << newType << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                        return;
                    }
                    if (datFileExists && !newType.empty() && newType != field["type"].get<string>()) {
                        cerr << "存在数据文件，禁止修改字段类型: " << oldFieldName << endl;
                        output << "存在数据文件，禁止修改字段类型: " << oldFieldName << endl;
                        return;
                    }
                    // 更新字段信息到 JSON 数据
                    if (!newFieldName.empty()) {
                        field["name"] = newFieldName;
                    }
                    if (!newType.empty()) {
                        field["type"] = newType;
                    }

                    // 保存修改后的 JSON 数据
                    ofstream outFile(filePath);
                    if (outFile.is_open()) {
                        outFile << dbJson.dump(4);
                        outFile.close();
                    }
                    else {
                        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
                        output << "无法打开文件 " << filePath << " 进行写入。" << endl;
                        return;
                    }

                    // 如果 `dat` 文件存在，处理字段名和类型的约束
                    if (datFileExists) {
                        vector<string> fileLines;
                        string line;
                        while (getline(datFile, line)) {
                            fileLines.push_back(line);
                        }

                        if (fileLines.size() < 4) {
                            cerr << "表文件格式不正确，行数不足 4 行。" << endl;
                            output << "表文件格式不正确，行数不足 4 行。" << endl;
                            datFile.close();
                            return;
                        }

                        // 解析第三行字段名
                        stringstream ss(fileLines[3]);
                        string cell;
                        vector<string> fields;
                        while (getline(ss, cell, ',')) {
                            fields.push_back(cell);
                        }

                        auto it = find(fields.begin(), fields.end(), oldFieldName);
                        if (it == fields.end()) {
                            cerr << "在表文件中未找到字段名称: " << oldFieldName << endl;
                            output << "在表文件中未找到字段名称: " << oldFieldName << endl;
                            datFile.close();
                            return;
                        }

                        // 更新字段名称
                        if (!newFieldName.empty()) {
                            *it = newFieldName;
                        }

                        // 检查类型转换规则
                        string currentType = field["type"];
                        if (!newType.empty() && currentType != newType) {
                            if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos) && newType.find("string") != string::npos) {
                                // 允许从 int 或 char 转换为 string，直接跳过
                            }
                            else if (currentType.find("string") != string::npos && (newType.find("int") != string::npos || newType.find("char") != string::npos)) {
                                cerr << "不允许将字段类型从 string 转换为 " << newType << "。" << endl;
                                output << "不允许将字段类型从 string 转换为 " << newType << "。" << endl;
                                return;
                            }
                            else if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos || currentType.find("string") != string::npos) &&
                                newType.find(currentType.substr(0, currentType.find('['))) != string::npos) {
                                // 同类型大小变化允许，直接跳过
                            }
                            else {
                                cerr << "不允许将字段类型从 " << currentType << " 转换为 " << newType << "。" << endl;
                                output << "不允许将字段类型从 " << currentType << " 转换为 " << newType << "。" << endl;
                                return;
                            }
                        }

                        // 更新第四行内容
                        string updatedFields;
                        for (size_t i = 0; i < fields.size(); ++i) {
                            updatedFields += fields[i];
                            if (i != fields.size() - 1) {
                                updatedFields += ",";
                            }
                        }
                        fileLines[3] = updatedFields;

                        // 写回修改后的内容
                        ofstream outFile(datFilePath, ios::trunc | ios::binary);
                        for (const auto& updatedLine : fileLines) {
                            outFile << updatedLine << '\n';
                        }
                        outFile.close();
                    }

                    cout << "字段 " << oldFieldName << " 已成功修改为 " << (newFieldName.empty() ? oldFieldName : newFieldName);
                    output << "字段 " << oldFieldName << " 已成功修改为 " << (newFieldName.empty() ? oldFieldName : newFieldName);
                    if (!newType.empty()) {
                        cout << "，类型修改为 " << newType;
                        output << "，类型修改为 " << newType;
                    }
                    cout << endl;
                    output << endl;
                    datFile.close();
                    // 调用汇总函数
                    tables = databaseTables[databaseName];
                    consolidateToDatabaseFile(databaseName, tables);
                    return;
                }
            }

            if (!fieldFound) { // 如果未找到字段
                cerr << "未找到字段名称: " << oldFieldName << endl;
                output << "未找到字段名称: " << oldFieldName << endl;
                return;
            }


        }
    }

    cerr << "未找到表: " << tableName << endl;
    output << "未找到表: " << tableName << endl;

}
//修改字段名或类型封装//
void handleModifyField(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 正则表达式支持三种情况：
    // 1. 修改字段名和类型: MODIFY FIELD oldName TO newName TYPE newType IN tableName IN databaseName;
    // 2. 仅修改字段名: MODIFY FIELD oldName TO newName IN tableName IN databaseName;
    // 3. 仅修改字段类型: MODIFY FIELD oldName TYPE newType IN tableName IN databaseName;
    regex modifyRegexFull(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    regex modifyRegexNameOnly(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    regex modifyRegexTypeOnly(R"(MODIFY FIELD (\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);

    smatch match;

    if (regex_match(sql, match, modifyRegexFull)) {
        // 修改字段名和类型
        string oldFieldName = match[1];
        string newFieldName = match[2];
        string newType = match[3];
        string tableName = match[4];
        string databaseName = match[5];
        modifyFieldInTable(tableName, databaseName, oldFieldName, newFieldName, newType, databaseTables);
    }
    else if (regex_match(sql, match, modifyRegexNameOnly)) {
        // 仅修改字段名
        string oldFieldName = match[1];
        string newFieldName = match[2];
        string tableName = match[3];
        string databaseName = match[4];
        modifyFieldInTable(tableName, databaseName, oldFieldName, newFieldName, "", databaseTables);
    }
    else if (regex_match(sql, match, modifyRegexTypeOnly)) {
        // 仅修改字段类型
        string oldFieldName = match[1];
        string newType = match[2];
        string tableName = match[3];
        string databaseName = match[4];
        modifyFieldInTable(tableName, databaseName, oldFieldName, "", newType, databaseTables);
    }
    else {
        cerr << "MODIFY FIELD 语法错误：" << sql << endl;
        output << "MODIFY FIELD 语法错误：" << sql << endl;
    }
}

void handleInsertRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 定义 INSERT INTO 的正则表达式
    regex insertRegex(R"(INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(([^)]+)\)\s+IN\s+(\S+)\s*;)", regex_constants::icase);
    smatch match;

    // 检查 SQL 语句是否匹配
    if (regex_match(sql, match, insertRegex)) {
        string tableName = match[1]; // 表名
        string valuesStr = match[2]; // 字段值
        string databaseName = match[3]; // 数据库名

        // 检查是否需要加载数据库
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            output<< "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".dbf"); // 假设 JSON 文件路径为 "MyDB.dbf"
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
                output<< "加载数据库失败或数据库不存在：" << databaseName << endl;
                return;
            }
        }
        // 将字段值分割成 vector
        vector<string> values;
        stringstream ss(valuesStr);
        string value;
        while (getline(ss, value, ',')) {
            value.erase(0, value.find_first_not_of(" "));
            value.erase(value.find_last_not_of(" ") + 1);
            values.push_back(value);
        }

        // 调用插入记录的函数
        insertRecord(databaseName, tableName, values, databaseTables);
    }
    else {
        cerr << "INSERT INTO 语法错误：" << sql << endl;
        output << "INSERT INTO 语法错误：" << sql << endl;
    }
}

void handleDeleteRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 定义 DELETE FROM 的正则表达式，支持多个条件
    regex deleteRegex(R"(DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+?)\s+IN\s+(\S+)\s*;)", regex_constants::icase);
    smatch match;

    // 检查 SQL 语句是否匹配
    if (regex_match(sql, match, deleteRegex)) {
        string tableName = match[1];     // 表名
        string whereClause = match[2];  // WHERE 子句
        string databaseName = match[3]; // 数据库名

        // 检查是否需要加载数据库
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".dbf"); // 假设 JSON 文件路径为 "MyDB.dbf"
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
                output << "加载数据库失败或数据库不存在：" << databaseName << endl;
                return;
            }
        }

        // 解析 WHERE 子句为条件集合
        vector<pair<string, string>> conditions;
        regex conditionRegex(R"((\w+)=([^\s]+))", regex_constants::icase);
        auto whereBegin = sregex_iterator(whereClause.begin(), whereClause.end(), conditionRegex);
        auto whereEnd = sregex_iterator();

        for (auto it = whereBegin; it != whereEnd; ++it) {
            conditions.emplace_back((*it)[1], (*it)[2]); // 字段名和值
        }

        if (conditions.empty()) {
            cerr << "WHERE 子句解析失败或为空：" << whereClause << endl;
            output << "WHERE 子句解析失败或为空：" << whereClause << endl;
            return;
        }

        // 调用删除记录的函数
        deleteRecordFromTableFile(databaseName, tableName, conditions, databaseTables);
    }
    else {
        cerr << "DELETE FROM 语法错误：" << sql << endl;
        output << "DELETE FROM 语法错误：" << sql << endl;
    }
}

void handleUpdateRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 定义 UPDATE 的正则表达式，支持多个条件和括号
    regex updateRegex(
        R"(UPDATE\s+(\w+)\s*\(\s*SET\s+(\w+)\s*=\s*([^)]+)\s+WHERE\s+(.+?)\s*\)\s+IN\s+(\S+)\s*;)",
        regex_constants::icase
    );
    smatch match;

    // 检查 SQL 语句是否匹配
    if (regex_match(sql, match, updateRegex)) {
        string tableName = match[1];        // 表名
        string targetField = match[2];     // 要修改的字段
        string newValue = trim(match[3]);  // 新值
        string whereClause = match[4];     // WHERE 子句
        string databaseName = match[5];    // 数据库名

        // 检查是否需要加载数据库
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            output<< "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".dbf");
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
                output<< "加载数据库失败或数据库不存在：" << databaseName << endl;
                return;
            }
        }

        // 解析 WHERE 子句为条件集合
        vector<pair<string, string>> conditions;
        regex conditionRegex(R"((\w+)\s*=\s*([^\s]+))", regex_constants::icase);
        auto whereBegin = sregex_iterator(whereClause.begin(), whereClause.end(), conditionRegex);
        auto whereEnd = sregex_iterator();

        for (auto it = whereBegin; it != whereEnd; ++it) {
            conditions.emplace_back((*it)[1], (*it)[2]); // 字段名和值
        }

        if (conditions.empty()) {
            cerr << "WHERE 子句解析失败或为空：" << whereClause << endl;
            output<< "WHERE 子句解析失败或为空：" << whereClause << endl;
            return;
        }

        // 调用更新记录的函数
        updateRecordInTableFile(databaseName, tableName, targetField, newValue, conditions, databaseTables);
    }
    else {
        cerr << "UPDATE 语法错误：" << sql << endl;
        output << "UPDATE 语法错误：" << sql << endl;
    }
}

void handleShowContent(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 定义 SHOW CONTENT 的正则表达式
    regex showRegex(R"(SHOW\s+CONTENT\s+(\w+)\s+IN\s+(\S+)\s*;)", regex_constants::icase);
    smatch match;

    // 检查 SQL 语句是否匹配
    if (regex_match(sql, match, showRegex)) {
        string tableName = match[1];      // 表名
        string databaseName = match[2];  // 数据库名

        // 检查是否需要加载数据库
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".dbf");
            //output << "isEmpty:" << databaseTables.empty() << endl;
            //output << "hasDB:" << databaseTables["DB"][0].tableName << endl;
            //output << "hasC:/.../DB:" << databaseTables["C:/my_codes/MYDBSM/DB"][0].tableName << endl;
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
                output<< "加载数据库失败或数据库不存在：" << databaseName << endl;
                return;
            }
        }

        // 在数据库中查找表
        auto& tables = databaseTables[databaseName];
        auto tableIt = find_if(tables.begin(), tables.end(), [&](const Table& t) {
            return t.tableName == tableName;
            });

        if (tableIt == tables.end()) {
            cerr << "表 " << tableName << " 不存在于数据库 " << databaseName << " 中。" << endl;
            output<< "表 " << tableName << " 不存在于数据库 " << databaseName << " 中。" << endl;
            return;
        }

        // 打开 .dat 文件并读取数据
        string datFilePath = databaseName + "_" + tableName + ".dat";
        ifstream datFile(datFilePath, ios::in | ios::binary);

        if (!datFile.is_open()) {
            cerr << "表文件 " << datFilePath << " 不存在。" << endl;
            output<< "表文件 " << datFilePath << " 不存在。" << endl;
            return;
        }

        cout << "表 " << tableName << " 的有效记录：" << endl;
        output << "表 " << tableName << " 的有效记录：" << endl;
        writeResult();
        // 跳过表名、记录数量和字段名行
        string line;
        getline(datFile, line); // 表名行
        getline(datFile, line); // 记录数量行
        getline(datFile, line); // 字段数量行
        getline(datFile, line); // 字段名行
        cout << line << endl;   // 输出字段名
        output<< line << endl;   // 输出字段名
        // 读取并展示所有有效记录
        while (getline(datFile, line)) {
            if (line.empty()) continue;
            vector<string> recordFields;
            stringstream ss(line);
            string fieldValue;

            // 按逗号分隔记录字段
            while (getline(ss, fieldValue, ',')) {
                recordFields.push_back(trim(fieldValue)); // 去掉多余空格
            }

            // 检查有效位
            if (!recordFields.empty() && recordFields[0] == "1") { // "1" 表示有效
                for (size_t i = 0; i < recordFields.size(); ++i) { // 不跳过有效位字段
                    cout << recordFields[i];
                    output<< recordFields[i];
                    if (i < recordFields.size() - 1)
                    {
                        cout << ", ";
                        output << ",";
                    }
                        
                }
                cout << endl;
                output<< endl;
            }
        }

        datFile.close();
    }
    else {
        cerr << "SHOW CONTENT 语法错误：" << sql << endl;
        output<< "SHOW CONTENT 语法错误：" << sql << endl;
    }
}


vector<vector<string>> readTable(string databaseName, string tableName) {
    // 检查是否需要加载数据库
    vector<vector<string>> res;
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        output << "检测到数据库尚未加载，尝试加载数据库：" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        //output << "isEmpty:" << databaseTables.empty() << endl;
        //output << "hasDB:" << databaseTables["DB"][0].tableName << endl;
        //output << "hasC:/.../DB:" << databaseTables["C:/my_codes/MYDBSM/DB"][0].tableName << endl;
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
            output << "加载数据库失败或数据库不存在：" << databaseName << endl;
            return res;
        }
    }

    // 在数据库中查找表
    auto& tables = databaseTables[databaseName];
    auto tableIt = find_if(tables.begin(), tables.end(), [&](const Table& t) {
        return t.tableName == tableName;
        });

    if (tableIt == tables.end()) {
        cerr << "表 " << tableName << " 不存在于数据库 " << databaseName << " 中。" << endl;
        output << "表 " << tableName << " 不存在于数据库 " << databaseName << " 中。" << endl;
        return res;
    }

    // 打开 .dat 文件并读取数据
    string datFilePath = databaseName + "_" + tableName + ".dat";
    ifstream datFile(datFilePath, ios::in | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output << "表文件 " << datFilePath << " 不存在。" << endl;
        return res;
    }

    cout << "表 " << tableName << " 的有效记录：" << endl;
    output << "表 " << tableName << " 的有效记录：" << endl;
    writeResult();
    // 跳过表名、记录数量和字段名行
    string line;
    getline(datFile, line); // 表名行
    getline(datFile, line); // 记录数量行
    getline(datFile, line); // 字段数量行
    getline(datFile, line); // 字段名行
    cout << line << endl;   // 输出字段名
    output << line << endl;   // 输出字段名
    // 读取并展示所有有效记录
    while (getline(datFile, line)) {
        if (line.empty()) continue;
        vector<string> recordFields;
        stringstream ss(line);
        string fieldValue;

        // 按逗号分隔记录字段
        while (getline(ss, fieldValue, ',')) {
            recordFields.push_back(trim(fieldValue)); // 去掉多余空格
        }

        // 检查有效位
        if (!recordFields.empty() && recordFields[0] == "1") { // "1" 表示有效
            vector<string> temp;
            for (size_t i = 0; i < recordFields.size(); ++i) { // 不跳过有效位字段
                cout << recordFields[i];
                output << recordFields[i];
                if (i < recordFields.size() - 1)
                {
                    cout << ", ";
                    output << ",";
                }
                if (i != 0)
                    temp.push_back(recordFields[i]);

            }
            res.push_back(temp);
            cout << endl;
            output << endl;
        }
    }

    datFile.close();
    return res;
}


vector<string> dbNames;
vector<string> tableNames;
//vector<string> tableF, tableN;
vector<string> fieldNames;
vector<string> originFieldNames;

void handleSelectContent_USE(const string& sql, map<string, vector<Table>>& databaseTables) {
    dbNames.clear();

    string tsql = sql;
    while (sql[0] == ' ') {
        tsql = tsql.substr(1);
    }

    output << tsql << endl;
    // 检查是否以 "USE " 开头
    if (tsql.size() < 4 ) {
        return;
    }
    string prefix = tsql.substr(0, 4);
    transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
    if (prefix != "use ")
        return;
    // 去掉 "USE " 前缀
    string remaining = tsql.substr(4);
    /*
    // 检查最后一个字符是否是 ';'
    if (remaining.back() != ';') {
        return;
    }
    // 去掉末尾的 ';'
    remaining.pop_back();
    */
    // 按照逗号分隔 DB 名称
    std::istringstream stream(remaining);
    std::string dbName;
    while (std::getline(stream, dbName, ',')) {
        // 去掉多余的空格
        size_t start = dbName.find_first_not_of(" \t");
        size_t end = dbName.find_last_not_of(" \t");

        // 检查有效性（去除特殊字符的数据库名称）
        if (start == std::string::npos || end == std::string::npos) {
            return ; // 空或无效名称
        }

        dbName = dbName.substr(start, end - start + 1);

        /*/ 确保没有特殊字符
        for (char c : dbName) {
            if (!std::isalnum(c) && c != '_') {
                return false; // 含有非法字符
            }
        }*/
        // 检查是否需要加载数据库
        databaseTables.clear();
        if (databaseTables.find(dbName) == databaseTables.end()) {
            cout << "检测到数据库尚未加载，尝试加载数据库：" << dbName << endl;
            output << "检测到数据库尚未加载，尝试加载数据库：" << dbName << endl;
            databaseTables = loadDatabaseTables(dbName + ".dbf");
            //output << "isEmpty:" << databaseTables.empty() << endl;
            //output << "hasDB:" << databaseTables["DB"][0].tableName << endl;
            //output << "hasC:/.../DB:" << databaseTables["C:/my_codes/MYDBSM/DB"][0].tableName << endl;
            if (databaseTables.find(dbName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << dbName << endl;
                output << "加载数据库失败或数据库不存在：" << dbName << endl;
                return;
            }
            else {
                output << "数据库：" << dbName << "已成功加载" << endl;
            }
        }
        else {
            output<< "数据库：" << dbName<<"已成功加载" << endl;
        }
        // 保存有效 DB 名称
        dbNames.push_back(dbName);
        output << "using database " << dbName<<endl;
    }
}

void handleSelectContent_SELECT(const string& sql, map<string, vector<Table>>& databaseTables) {
    fieldNames.clear();

    // 检查是否以 "SELECT " 开头
    if (sql.size() < 7) {
        return;
    }
    string prefix = sql.substr(0, 7);
    transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
    if (prefix != "select ")
        return;
    // 去掉 "select " 前缀
    string remaining = sql.substr(7);
    // 检查最后一个字符是否是 ';'
    /*
    if (remaining.back() != ';') {
        return;
    }
    // 去掉末尾的 ';'
    remaining.pop_back();
    */
    // 按照逗号分隔 FIELD 名称
    std::istringstream stream(remaining);
    std::string fdName;
    while (std::getline(stream, fdName, ',')) {
        // 去掉多余的空格
        size_t start = fdName.find_first_not_of(" \t");
        size_t end = fdName.find_last_not_of(" \t");

        // 检查有效性（去除特殊字符的数据库名称）
        if (start == std::string::npos || end == std::string::npos) {
            return; // 空或无效名称
        }

        fdName = fdName.substr(start, end - start + 1);

        // 保存有效 field 名称
        fieldNames.push_back(fdName);
        output << "using field: " << fdName<<endl;
    }
}

void handleSelectContent_FROM(const string& sql, map<string, vector<Table>>& databaseTables) {
    tableNames.clear();

    // 检查是否以 "FROM " 开头
    if (sql.size() < 5) {
        return;
    }
    string prefix = sql.substr(0, 5);
    transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
    if (prefix != "from ")
        return;
    // 去掉 "from " 前缀
    string remaining = sql.substr(5);
    // 检查最后一个字符是否是 ';'
    /*
    if (remaining.back() != ';') {
        return;
    }
    // 去掉末尾的 ';'
    remaining.pop_back();
    */
    // 按照逗号分隔 FIELD 名称
    std::istringstream stream(remaining);
    std::string tbName;
    while (std::getline(stream, tbName, ',')) {
        // 去掉多余的空格
        size_t start = tbName.find_first_not_of(" \t");
        size_t end = tbName.find_last_not_of(" \t");

        // 检查有效性（去除特殊字符的数据库名称）
        if (start == std::string::npos || end == std::string::npos) {
            return; // 空或无效名称
        }

        tbName = tbName.substr(start, end - start + 1);

        // 保存有效 tb 名称
        tableNames.push_back(tbName);
        output << "using table: " << tbName << endl;
    }
}


vector<string>splitName(string s) {
    vector<string>res;
    size_t start = 0;
    size_t pos = 0;
    int max_splits = 3; // 最多处理3个分隔符，生成4部分

    while ((pos = s.find('.', start)) != std::string::npos && max_splits > 0) {
        res.push_back(s.substr(start, pos - start)); // 提取当前部分
        start = pos + 1; // 更新起始位置
        max_splits--;    // 减少剩余分隔符处理次数
    }
    // 添加最后一部分
    res.push_back(s.substr(start));
    return res;
}

unordered_set<char> charSet = { '+','-','*','/','(',')',' ','%','=','>','<'};
std::string addSpacesAroundCharacters(const std::string& input, const std::unordered_set<char>& charSet) {
    std::string result;

    for (char c : input) {
        if (charSet.find(c) != charSet.end()) {
            result += " ";
            result += c;
            result += " ";
        }
        else {
            result += c;
        }
    }

    // 清理可能产生的多余空格（例如多个字符连续时）
    std::string cleanedResult;
    bool lastWasSpace = false;

    for (char c : result) {
        if (c == ' ') {
            if (!lastWasSpace) {
                cleanedResult += c;
            }
            lastWasSpace = true;
        }
        else {
            cleanedResult += c;
            lastWasSpace = false;
        }
    }

    return cleanedResult;
}

void expandAsterisk(std::vector<std::string>& vec, const std::vector<std::string>& insertVec) {
    for (size_t i = 0; i < vec.size(); ++i) {
        if (vec[i] == "*") {
            // 插入 insertVec 到当前位置
            vec.erase(vec.begin() + i); // 先移除 "*"
            vec.insert(vec.begin() + i, insertVec.begin(), insertVec.end()); // 插入 insertVec
            i += insertVec.size() - 1; // 调整索引，跳过插入的部分
        }
    }
}

void handleSelectContent_WHERE(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 检查是否以 "WHERE " 开头
    //output << "handling where\n";
    //output << sql <<"\n";
    if (sql.size() < 6) {
        return;
    }
    //output << sql.size() << "\n";
    string prefix = sql.substr(0, 6);
    transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
    //output << prefix << endl;
    if (prefix != "where ")
        return;
    // 去掉 "where " 前缀
    string remaining = "WHERE 1 = ( "+sql.substr(6);
    // 检查最后一个字符是否是 ';'
    if (remaining.back() != ';') {
        remaining = remaining + " );";
    }
    else {
        remaining = remaining.substr(0,remaining.size()-1) + " );";
    }
    remaining = addSpacesAroundCharacters(remaining, charSet);
    output << remaining<<endl;

   
    originFieldNames = fieldNames;
    //规范化变量名为：tablenames: dbn.tbn;fieldnames:dbn.tbn.fdn;

    output << "原始field命名为：" << endl;
    for (int i = 0; i < fieldNames.size(); i++) {
        output << fieldNames[i] << ",";
    }


    if (dbNames.size() == 1) {//为1时可以省略变量名的dbname
        for (int i = 0; i < tableNames.size(); i++) {
            if (tableNames[i].rfind(dbNames[0], 0) == 0&& tableNames[i].size()> dbNames[0].length() && tableNames[i][dbNames[0].length()] == '.') {
                continue;
            }
            else {
                if (tableNames[i] != "*") {
                    tableNames[i] = dbNames[0] + "." + tableNames[i];
                }
            }
        }
        if (tableNames.size() == 1) {//均为1时可以省略tablename
            for(int i=0;i<fieldNames.size();i++) {
                if (fieldNames[i].rfind(tableNames[0], 0) == 0 && fieldNames[i].size() > tableNames[0].length() && fieldNames[i][tableNames[0].length()] == '.') {
                    continue;
                }
                else {
                    if (fieldNames[i] != "*") {
                        fieldNames[i] = tableNames[0] + "." + fieldNames[i];
                    }
                }
            }
        }
    }



    map<string, map<string, vector<vector<string>>>>cache;//dbname:tablename:[row][values]
    


    for (int j = 0; j < tableNames.size(); j++) {
        auto t = splitName(tableNames[j]);
        auto vvs=readTable(t[0],t[1]);
        cache[t[0]][t[1]] = vvs;
    }


    
    //处理“*”：从规范tableNames读取DBname.tableName然后匹配成功后将其所有字段加入allFields
    vector<string> allFields;
    for (auto it : tableNames) {//遍历所有用到的table
        vector<string> sit = splitName(it);
        string itDB = sit[0];
        string itTB = sit[1];
        for (auto dt = databaseTables.begin(); dt != databaseTables.end(); ++dt) {//从database中找
            output << "当前Database:" << dt->first << endl;
            if (dt->first == itDB) {
                output << "匹配Database成功！" << endl;
                for (int i = 0; i < dt->second.size(); i++) {
                    if (dt->second[i].tableName == itTB)
                    {
                        output << "匹配Table成功！" << endl;
                        for (auto fdn : dt->second[i].fields) {
                            string itFN = fdn.name;
                            allFields.push_back(itDB + "." + itTB + "." + itFN);
                        }
                    }
                }
            }
        }
    }
    output << "FieldNames :";
    for (auto x : fieldNames) {
        output << x << ",";
    }
    output << endl;
    output << "originNames :";
    for (auto x : originFieldNames) {
        output << x << ",";
    }
    output << endl;
    expandAsterisk(fieldNames, allFields);
    expandAsterisk(originFieldNames, allFields);
    output << "modifiedFieldNames :";
    for (auto x : fieldNames) {
        output << x << ",";
    }
    output << endl;
    output << "modifiedOriginNames :";
    for (auto x : originFieldNames) {
        output << x << ",";
    }


    vector<vector<string>> finalResult;
    finalResult.push_back(originFieldNames);

    /*
    for (auto it = databaseTables.begin(); it != databaseTables.end(); ++it) {
        output << "当前Database:" << it->first << endl;
        if (it->first == itDB->first) {
            output << "匹配Database成功！" << endl;
            for (int i = 0; i < it->second.size(); i++) {
                if (it->second[i].tableName == itTB->first)
                {
                    output << "匹配Table成功！" << endl;
                    for (int k = 0; k < val.size(); k++) {
                        if (dbNames.size() == 1 && tableNames.size() == 1) {
                            variableNamesString[it->second[i].fields[k].name] = val[k];
                            try {
                                variableNames[it->second[i].fields[k].name] = stod(val[k]);
                            }
                            catch (...) {
                                output << "不适配比较的数据类型" << endl;
                            }
                        }/*
    for (auto itDB = cache.begin(); itDB != cache.end(); ++itDB) {
        // 使用it->first和it->second访问键和值
        for (auto itTB = itDB->second.begin(); itTB != itDB->second.end(); ++itTB) {
            if (itTB->second.size() > 0) {
                for (int t = 0; t < itTB->second.size(); t++) {
                    allFields.push_back(itDB->first + itTB->first + itTB->second[t]);
                }
                //totalRow *= itTB->second.size();
            }
        }
    }
    */

    




    output << "成功规范化命名为：" << endl;
    for (int i = 0; i < fieldNames.size(); i++) {
        output << fieldNames[i] << ",";
    }


    long long totalRow = 1;
    for (auto itDB = cache.begin(); itDB != cache.end(); ++itDB) {
        // 使用it->first和it->second访问键和值
        for (auto itTB = itDB->second.begin(); itTB != itDB->second.end(); ++itTB) {
            if (itTB->second.size() > 0) {
                totalRow *= itTB->second.size();
            }
        }
    }
    output << "总查询行次数为" << totalRow << endl;

    std::unordered_map<std::string, double> variableNames;
    std::unordered_map<std::string, string> variableNamesString;
    for (long long u = 0; u < totalRow; u++) {
        long long tid = u;//当前行序列号
        for (auto itDB = cache.begin(); itDB != cache.end(); ++itDB) {//遍历DataBase
            // 使用it->first和it->second访问键和值
            for (auto itTB = itDB->second.begin(); itTB != itDB->second.end(); ++itTB) {//遍历Table
                if (itTB->second.size() > 0) {

                    output << "正在处理" << itDB->first << "." << itTB->first << endl;

                    //totalRow *= itTB->second.size();
                    vector<string> val = itTB->second[tid % itTB->second.size()];//val是一行

                    output << "当前行数值为:";
                    for (int tit = 0; tit < val.size(); tit++) {
                        output << val[tit] << ",";
                    }
                    output << endl;

                    output << "读取表头中...\n";
                    for (auto it = databaseTables.begin(); it != databaseTables.end(); ++it) {
                        output << "当前Database:" << it->first << endl;
                        if (it->first == itDB->first) {
                            output << "匹配Database成功！" << endl;
                            for (int i = 0; i < it->second.size(); i++) {
                                if (it->second[i].tableName == itTB->first)
                                {
                                    output << "匹配Table成功！" << endl;
                                    for (int k = 0; k < val.size(); k++) {
                                        if (dbNames.size() == 1 && tableNames.size() == 1) {
                                            variableNamesString[it->second[i].fields[k].name] = val[k];
                                            try {
                                                variableNames[it->second[i].fields[k].name] = stod(val[k]);
                                            }
                                            catch (...) {
                                                output << "不适配比较的数据类型" << endl;
                                            }
                                        }
                                            
                                        if (dbNames.size() == 1) {
                                            variableNamesString[itTB->first + "." + it->second[i].fields[k].name] = val[k];
                                            try {
                                                variableNames[itTB->first + "." + it->second[i].fields[k].name] = stod(val[k]);
                                            }
                                            catch (...) {
                                                output << "不适配比较的数据类型" << endl;
                                            }
                                        }
                                        variableNamesString[itDB->first + "." + itTB->first + "." + it->second[i].fields[k].name] = val[k];
                                        try {
                                            variableNames[itDB->first + "." + itTB->first + "." + it->second[i].fields[k].name] = stod(val[k]);
                                        }
                                        catch (...) {
                                            output << "不适配比较的数据类型" << endl;
                                        }
                                    }
                                }
                            }
                        }  
                    }
                    
                    tid /= itTB->second.size();
                }
            }
        }



        output << "当前行:";
        for (auto tit = variableNames.begin(); tit != variableNames.end(); ++tit) {
            output << " " << tit->first<<":"<<tit->second;
        }
        output << endl;
        output << "指令为："<<remaining << endl;
        bool result;
        try {
            result = parseWhereStatement(remaining, variableNames);
            vector<string> tempvs;
            for (int j = 0; j < originFieldNames.size(); j++) {
                output << originFieldNames[j] << ":" << variableNamesString[originFieldNames[j]] << "\t";
                if (result) {
                    tempvs.push_back(variableNamesString[originFieldNames[j]]);
                }
            }
            finalResult.push_back(tempvs);
            output << endl;
            output << "选取结果为:" << ((result == true) ? "true" : "false")<<endl;
            //writeResult();
        }
        catch (const std::exception& ex) {
            output << "Parse Failed\n";
            output << "Error: " << ex.what() << std::endl;
        }
        
    }
    writeResult();
    output << "show Table";
    writeResult();
    for (int i = 0; i < finalResult.size(); i++) {
        for (int j = 0; j < finalResult[i].size(); j++) {
            if(j==0)
                output << finalResult[i][j];
            else
                output << ","<<finalResult[i][j];
        }
        if (i == 0)
            output << ",\n";
        else
            if(i!=finalResult.size()-1)
                output << "\n";
    }
    output << endl;
    writeResult();
    /*
    
    use DB
    select *
    from MyTable
    where Age > 0;
    */
    /*
    use DB
    select Age,Name
    from MyTable
    where Age = 9;
    */  
    /*
    use DB
select MyTable.Name,MyTable.Age,xiaoboshiTable.XIAOBOSHI2
from MyTable,xiaoboshiTable
where MyTable.Age=xiaoboshiTable.Age;
    */
    /*variableNames = {
        {"x", 5},
        {"y", 10},
        {"z", 15}
    };

    std::string input = "WHERE (x + y > z) and (y < 20);";
    bool result;

    if (parseWhereStatement(input, variableNames, result)) {
        std::cout << "Expression result: " << (result ? "true" : "false") << std::endl;
        output<< "Expression result: " << (result ? "true" : "false") << std::endl;
    }
    else {
        std::cout << "Failed to parse WHERE statement." << std::endl;
        output << "Failed to parse WHERE statement." << std::endl;
    }
    */
    /*
    variableNames = {
        {"x", 10},
        {"y", 5},
        {"z", 20}
    };

    std::string input = "WHERE ( x + y ) * z > 100 AND NOT ( y < 2 );";

    try {
        bool result = parseWhereStatement(input, variableNames);
        std::cout << "Expression result: " << std::boolalpha << result << std::endl;
        output<< "Expression result: " << std::boolalpha << result << std::endl;
    }
    catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        output<< "Error: " << ex.what() << std::endl;
    }
    */

}

//函数处理
void processSQLCommands(const string& tsql, map<string, vector<Table>>& databaseTables) {
    try {
        //output << "收到的sql@" << tsql << "@" << endl;
        string sql = tsql;
        sql = trim(sql);
        //output << "现在的sql@" << sql << "@" << endl;
        if (containsIgnoreCase(sql, "CREATE TABLE")) {
            handleCreateTable(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "EDIT TABLE")) {
            handleEditTable(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "RENAME TABLE")) {
            handleRenameTable(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "DROP TABLE")) {
            handleDropTable(sql);
        }
        else if (containsIgnoreCase(sql, "READ DATABASE")) {
            handleReadDatabase(sql);
        }
        else if (containsIgnoreCase(sql, "ADD FIELD")) {
            handleAddField(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "REMOVE FIELD")) {
            handleRemoveField(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "MODIFY FIELD")) {
            handleModifyField(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "INSERT")) {
            handleInsertRecord(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "DELETE")) {
            handleDeleteRecord(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "UPDATE")) {
            handleUpdateRecord(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "SHOW")) {
            handleShowContent(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "USE")) {
            handleSelectContent_USE(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "SELECT")) {
            handleSelectContent_SELECT(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "FROM")) {
            handleSelectContent_FROM(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "WHERE")) {
            handleSelectContent_WHERE(sql, databaseTables);
        }
        else if (sql == "save") {
            cout << "检测到输入save，保存数据。\n";
            output << "检测到输入save，保存数据。\n";
            saveTablesToDatabaseFiles(databaseTables); // 保存数据
        }
        else {
            cerr << "不支持的操作：" << sql << endl;
            output << "不支持的操作：" << sql << endl;
        }
    }
    catch (const exception& e) {
        cerr << "错误: " << e.what() << endl;
        output << "错误: " << e.what() << endl;
    }
}


/*
// 主函数：解析语句
int mainParse() {
    map<string, vector<Table>> databaseTables;

    cout << "请输入 SQL 语句（单行模式输入语句后按回车执行，或输入 `MULTI` 切换到多行模式）：\n";
    while (true) {
        string sql;
        getline(cin, sql);

        // 检测退出命令
        if (sql == "0") {
            cout << "检测到输入 0，保存数据并退出。\n";
            saveTablesToDatabaseFiles(databaseTables); // 保存数据
            break;
        }

        // 多行模式
        if (containsIgnoreCase(sql, "Multi")) {
            cout << "进入多行模式，请输入多条语句，每行一条，输入 `END` 结束多行输入。\n";
            vector<string> commands;
            while (true) {
                getline(cin, sql);
                if (containsIgnoreCase(sql, "END")) {
                    cout << "多行模式结束，依次执行输入的语句。\n";
                    break;
                }
                commands.push_back(sql);
            }
            for (const string& command : commands) {
                processSQLCommands(command, databaseTables);
            }
        }
        // 单行模式
        else {
            processSQLCommands(sql, databaseTables);
        }
    }

    return 0;
}
*/
void writeResult(string s)
{
    results.push_back(s);
}
void writeResult()
{
    results.push_back(output.str());
    output.str("");
}
vector<string> streamParse(string s) {
    //map<string, vector<Table>> databaseTables;
    results.clear();
    string sql2=s,sql;
    //cout << "进入多行模式，请输入多条语句，每行一条，输入 `END` 结束多行输入。\n";
    vector<string> commands;
    stringstream ss(sql2);
    while (std::getline(ss, sql,'\n')) {
        if (containsIgnoreCase(sql, "END") || sql == "END" || sql == "\nEND") {
            cout << "多行模式结束，依次执行输入的语句。\n";
            output<< "多行模式结束，依次执行输入的语句。\n";
            break;
        }
        commands.push_back(sql);
    }
    for (const string& command : commands) {
        processSQLCommands(command, databaseTables);
    }
    string filePath;
    for (auto dt = databaseTables.begin(); dt != databaseTables.end(); ++dt){
        string databaseName = dt->first;
        filePath = databaseName + ".dbf";
    }
    //output << "file is:" << filePath << endl;
    databaseTables = loadDatabaseTables(filePath);
    writeResult();
    return results;
}

