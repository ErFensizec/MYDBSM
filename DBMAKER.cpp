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
#include <algorithm>
#include <set>
#include "DBMAKER.h"
using namespace std;
using json = nlohmann::json;
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
    exit(1);
}

// 解析字段定义
Field parseField(const string& fieldLine) {
    regex fieldRegex(R"((\w+)\s+(\w+\[\d+\]|\w+)\s+(KEY|NOT_KEY)\s+(NO_NULL|NULL)\s+(VALID|NOT_VALID))",regex_constants::icase);
    smatch match;
    string trimmedField = trim(fieldLine);
    if (!regex_match(trimmedField, match, fieldRegex)) {
        throwError("Invalid field definition: " + fieldLine);
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
    }
    table.tableName = match[1];
    table.databaseName = match[3];
    string fieldsSection = match[2];
    istringstream fieldsStream(fieldsSection);
    string fieldLine;

    while (getline(fieldsStream, fieldLine, ',')) {
        Field field = parseField(fieldLine);
        table.fields.push_back(field);
    }
    return table;
}

// 保存表信息到对应数据库的 JSON 文件
// 保存表信息到对应数据库的 JSON 文件
void saveTablesToDatabaseFiles(const map<string, vector<Table>>& databaseTables) {
    for (const auto& [databaseName, tables] : databaseTables) {
        string filePath = databaseName + ".dbf";
        json dbJson;

        // 如果文件存在，读取现有数据
        ifstream inFile(filePath);
        if (inFile.is_open()) {
            try {
                inFile >> dbJson; // 解析已有 JSON 文件
            }
            catch (const exception& e) {
                cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
                output<< "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
                //writeResult();
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

        // 将新表数据添加到 JSON 中
        for (const auto& table : tables) {
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
            output<< "表信息已保存到 " << filePath << endl;
        }
        else {
            cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
            output<< "无法打开文件 " << filePath << " 进行写入。" << endl;
        }
    }
}


// 编辑表字段
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields) {
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
        cerr << "表 " << tableName << " 不存在。" << endl;
        output<< "表 " << tableName << " 不存在。" << endl;
        return;
    }

    // 查找表并编辑字段
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            tableJson["fields"] = json::array();  // 清空原有字段
            for (const auto& field : newFields) {
                json fieldJson;
                fieldJson["name"] = field.name;
                fieldJson["type"] = field.type;
                fieldJson["key_flag"] = field.keyFlag;
                fieldJson["null_flag"] = field.nullFlag;
                fieldJson["valid_flag"] = field.validFlag;
                tableJson["fields"].push_back(fieldJson);
            }
            found = true;
            break;
        }
    }

    if (!found) {
        cerr << "未找到表 " << tableName << "。" << endl;
        output<< "未找到表 " << tableName << "。" << endl;
        return;
    }

    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
        cout << "表 " << tableName << " 的字段已更新。\n";
        output<< "表 " << tableName << " 的字段已更新。\n";
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        output<< "无法打开文件 " << filePath << " 进行写入。" << endl;
    }
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

// 删除表
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
            output<< "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << tableName << " 不存在。" << endl;
        output<< "表 " << tableName << " 不存在。" << endl;
        return;
    }

    // 删除表
    auto& tables = dbJson["tables"];
    auto it = find_if(tables.begin(), tables.end(), [&](const json& tableJson) {
        return tableJson["table_name"] == tableName;
        });

    if (it != tables.end()) {
        tables.erase(it);
        cout << "表 " << tableName << " 已删除。\n";
        output<< "表 " << tableName << " 已删除。\n";
    }
    else {
        cerr << "未找到表 " << tableName << "。\n";
        output<< "未找到表 " << tableName << "。\n";
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
        output<< "无法打开文件 " << filePath << " 进行写入。" << endl;
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
    table.fieldCount = table.fields.size(); // 初始化字段数量
    databaseTables[table.databaseName].push_back(table);
    table.print();
    saveTablesToDatabaseFiles(databaseTables); // 保存表结构到文件
    cout << "表创建成功: " << table.tableName << "，字段数量: " << table.fieldCount << endl;
    output<< "表创建成功: " << table.tableName << "，字段数量: " << table.fieldCount << endl;
}

//表修改封装
void handleEditTable(const string& sql) {
    regex editRegex(R"(EDIT TABLE (\w+)\s*\(\s*(.+?)\s*\)\s*IN\s+(\w+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, editRegex)) {
        string tableName = match[1];
        string databaseName = match[3];
        string fieldsSection = match[2];

        vector<Field> newFields;
        istringstream fieldsStream(fieldsSection);
        string fieldLine;

        while (getline(fieldsStream, fieldLine, ',')) {
            Field field = parseField(fieldLine);
            newFields.push_back(field);
        }

        editTableFields(tableName, databaseName, newFields);
    }
    else {
        cerr << "EDIT TABLE 语法错误：" << sql << endl;
        output<< "EDIT TABLE 语法错误：" << sql << endl;
    }
}

//表重命名封装
void handleRenameTable(const string& sql) {
    regex renameRegex(R"(RENAME TABLE (\w+)\s+(\w+)\s+IN\s+(\w+);)", regex_constants::icase);
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
    regex dropRegex(R"(DROP TABLE (\w+)\s+IN\s+(\w+);)", regex_constants::icase);
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
    regex addRegex(R"(ADD FIELD (\w+)\s*\((.+)\)\s*IN\s+(\w+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, addRegex)) {
        string tableName = match[1];
        string databaseName = match[3];
        Field newField = parseField(match[2]);
        addFieldToTable(tableName, databaseName, newField);
    }
    else {
        cerr << "ADD FIELD 语法错误：" << sql << endl;
        output<< "ADD FIELD 语法错误：" << sql << endl;
    }
}

//删除字段封装
void handleRemoveField(const string& sql) {
    regex removeRegex(R"(REMOVE FIELD (\w+)\s+IN\s+(\w+)\s+IN\s+(\w+);)", regex_constants::icase);
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


// 从 JSON 文件加载数据库表信息
map<string, vector<Table>> loadDatabaseTables(const string& jsonFilePath) {
    ifstream inFile(jsonFilePath);
    if (!inFile.is_open()) {
        cerr << "无法打开文件: " << jsonFilePath << endl;
        return {};
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();

    map<string, vector<Table>> databaseTables;
    for (const auto& tableJson : dbJson["tables"]) {
        Table table;
        table.tableName = tableJson["table_name"].get<string>();
        table.databaseName = tableJson["database_name"].get<string>();
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

void consolidateToDatabaseFile(const string& databaseName, const vector<Table>& tables) {
    string databaseFilePath = databaseName + ".dat";
    ofstream databaseFile(databaseFilePath, ios::binary);

    if (!databaseFile.is_open()) {
        cerr << "无法创建主数据库文件: " << databaseFilePath << endl;
        output<< "无法创建主数据库文件: " << databaseFilePath << endl;
        return;
    }

    databaseFile << "# " << databaseName << '\n'; // 写入数据库标识

    for (const auto& table : tables) {
        string tableFilePath = databaseName + "_" + table.tableName + ".dat";
        ifstream tableFile(tableFilePath, ios::binary);

        if (!tableFile.is_open()) {
            cerr << "无法打开表文件: " << tableFilePath << endl;
            output<< "无法打开表文件: " << tableFilePath << endl;
            continue;
        }

        databaseFile << tableFile.rdbuf(); // 直接追加表文件内容到数据库文件
        databaseFile.put('\n');            // 表之间添加换行分隔
        tableFile.close();
    }

    databaseFile.close();
    cout << "所有表已成功汇总到数据库文件 " << databaseFilePath << " 中。" << endl;
    output<< "所有表已成功汇总到数据库文件 " << databaseFilePath << " 中。" << endl;
}

void insertRecord(const string& databaseName, const string& tableName, const vector<string>& values, map<string, vector<Table>>& databaseTables) {
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cerr << "数据库 " << databaseName << " 不存在。" << endl;
        output<< "数据库 " << databaseName << " 不存在。" << endl;
        return;
    }

    auto& tables = databaseTables[databaseName];
    auto it = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "表 " << tableName << " 不存在。" << endl;
        output<< "表 " << tableName << " 不存在。" << endl;
        return;
    }

    Table& table = *it;

    if (values.size() != table.fieldCount) {
        cerr << "插入的字段数量不匹配，表 " << tableName << " 需要 " << table.fieldCount << " 个字段。" << endl;
        output << "插入的字段数量不匹配，表 " << tableName << " 需要 " << table.fieldCount << " 个字段。" << endl;
        return;
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
    cout << "记录已插入表 " << tableName << "，并更新至数据库文件" << databaseName << endl;
    output<< "记录已插入表 " << tableName << "，并更新至数据库文件" << databaseName << endl;
}

void deleteRecordFromTableFile(const string& databaseName, const string& tableName, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output<< "表文件 " << datFilePath << " 不存在。" << endl;
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

void updateRecordInTableFile(const string& databaseName, const string& tableName, const string& targetField, const string& newValue, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
        output<< "表文件 " << datFilePath << " 不存在。" << endl;
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

    // 检查目标字段是否存在
    if (fieldIndexMap.find(targetField) == fieldIndexMap.end()) {
        cerr << "字段 " << targetField << " 不存在。" << endl;
        output<< "字段 " << targetField << " 不存在。" << endl;
        datFile.close();
        return;
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
            output<< "无法打开文件进行写回操作。" << endl;
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
        output<< "记录更新成功。" << endl;
    }
    else {
        cerr << "未找到满足条件的记录，或记录无效。" << endl;
        output<< "未找到满足条件的记录，或记录无效。" << endl;
    }
}

void handleInsertRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // 定义 INSERT INTO 的正则表达式
    regex insertRegex(R"(INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(([^)]+)\)\s+IN\s+(\w+)\s*;)", regex_constants::icase);
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
    regex deleteRegex(R"(DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+?)\s+IN\s+(\w+)\s*;)", regex_constants::icase);
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
        R"(UPDATE\s+(\w+)\s*\(\s*SET\s+(\w+)\s*=\s*([^)]+)\s+WHERE\s+(.+?)\s*\)\s+IN\s+(\w+)\s*;)",
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
    regex showRegex(R"(SHOW\s+CONTENT\s+(\w+)\s+IN\s+(\w+)\s*;)", regex_constants::icase);
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

//函数处理
void processSQLCommands(const string& sql, map<string, vector<Table>>& databaseTables) {
    try {
        if (containsIgnoreCase(sql, "CREATE TABLE")) {
            handleCreateTable(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "EDIT TABLE")) {
            handleEditTable(sql);
        }
        else if (containsIgnoreCase(sql, "RENAME TABLE")) {
            handleRenameTable(sql);
        }
        else if (containsIgnoreCase(sql, "DROP TABLE")) {
            handleDropTable(sql);
        }
        else if (containsIgnoreCase(sql, "READ DATABASE")) {
            handleReadDatabase(sql);
        }
        else if (containsIgnoreCase(sql, "ADD FIELD")) {
            handleAddField(sql);
        }
        else if (containsIgnoreCase(sql, "REMOVE FIELD")) {
            handleRemoveField(sql);
        }
        else if (containsIgnoreCase(sql, "MODIFY FIELD")) {
            handleModifyField(sql);
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
    writeResult();
    output.str("");
    return results;
}

