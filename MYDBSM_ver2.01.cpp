﻿#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <regex>
#include <fstream>
#include <nlohmann/json.hpp>
#include <map>
#include <algorithm>
#include <set>

using namespace std;
using json = nlohmann::json;

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
};

// 修剪字符串（去除前后空格）
string trim(const string& str) {
    size_t first = str.find_first_not_of(" \t");
    size_t last = str.find_last_not_of(" \t");
    return (first == string::npos || last == string::npos) ? "" : str.substr(first, last - first + 1);
}

// 错误处理
void throwError(const string& message) {
    cerr << "Error: " << message << endl;
    exit(1);
}

// 解析字段定义
Field parseField(const string& fieldLine) {
    regex fieldRegex(R"((\w+)\s+(\w+\[\d+\]|\w+)\s+(KEY|NOT_KEY)\s+(NO_NULL|NULL)\s+(VALID))",regex_constants::icase);
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
void saveTablesToDatabaseFiles(const map<string, vector<Table>>& databaseTables) {
    for (const auto& [databaseName, tables] : databaseTables) {
        string filePath = databaseName + ".json";
        json dbJson;

        // 如果文件存在，读取现有数据
        ifstream inFile(filePath);
        if (inFile.is_open()) {
            try {
                inFile >> dbJson; // 解析已有 JSON 文件
            }
            catch (const exception& e) {
                cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
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
                cerr << "表 \"" << table.tableName << "\" 已存在于 " << filePath << " 中，跳过此表。" << endl;
                continue;
            }
            dbJson["tables"].push_back(table.toJson());
        }

        // 写回文件
        ofstream outFile(filePath);
        if (outFile.is_open()) {
            outFile << dbJson.dump(4); // 使用 4 个空格缩进
            outFile.close();
            cout << "表信息已保存到 " << filePath << endl;
        }
        else {
            cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        }
    }
}

// 编辑表字段
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    json dbJson;

    // 读取现有数据
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // 解析已有 JSON 文件
        }
        catch (const exception& e) {
            cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << tableName << " 不存在。" << endl;
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
        return;
    }

    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
        cout << "表 " << tableName << " 的字段已更新。\n";
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
    }
}

// 更改表名
void renameTable(const string& oldTableName, const string& newTableName, const string& databaseName) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    json dbJson;

    // 读取现有数据
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // 解析已有 JSON 文件
        }
        catch (const exception& e) {
            cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << oldTableName << " 不存在。" << endl;
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
        return;
    }

    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
        cout << "表 " << oldTableName << " 已重命名为 " << newTableName << "。\n";
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
    }
}

// 删除表
void dropTable(const string& tableName, const string& databaseName) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    json dbJson;

    // 读取现有数据
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // 解析已有 JSON 文件
        }
        catch (const exception& e) {
            cerr << "读取文件 " << filePath << " 时发生错误：" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "表 " << tableName << " 不存在。" << endl;
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
    }
    else {
        cerr << "未找到表 " << tableName << "。\n";
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
    }
}
//显示模式
void readDatabaseFile(const string& databaseName) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        return;
    }

    try {
        json dbJson;
        inFile >> dbJson;
        inFile.close();

        if (!dbJson.contains("tables") || !dbJson["tables"].is_array()) {
            cerr << "数据库文件格式错误: 缺少 'tables' 字段。" << endl;
            return;
        }

        cout << "数据库: " << databaseName << endl;
        for (const auto& tableJson : dbJson["tables"]) {
            cout << "表名: " << tableJson["table_name"] << endl;
            cout << "字段:" << endl;
            for (const auto& field : tableJson["fields"]) {
                cout << "  - 名称: " << field["name"]
                    << ", 类型: " << field["type"]
                    << ", 是否主键: " << field["key_flag"]
                    << ", 可为空: " << field["null_flag"]
                    << ", 是否有效: " << field["valid_flag"] << endl;
            }
        }
    }
    catch (const exception& e) {
        cerr << "解析数据库文件时发生错误: " << e.what() << endl;
    }
}
//字段增加
void addFieldToTable(const string& tableName, const string& databaseName, const Field& newField) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
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
                    return;
                }
            }

            json fieldJson;
            fieldJson["name"] = newField.name;
            fieldJson["type"] = newField.type;
            fieldJson["key_flag"] = newField.keyFlag;
            fieldJson["null_flag"] = newField.nullFlag;
            fieldJson["valid_flag"] = newField.validFlag;

            tableJson["fields"].push_back(fieldJson);

            ofstream outFile(filePath);
            outFile << dbJson.dump(4);
            outFile.close();

            cout << "字段 " << newField.name << " 已成功添加到表 " << tableName << endl;
            return;
        }
    }

    cerr << "未找到表: " << tableName << endl;
}
//删除字段
void removeFieldFromTable(const string& tableName, const string& databaseName, const string& fieldName) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
        return;
    }

    json dbJson;
    inFile >> dbJson;
    inFile.close();

    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            auto& fields = tableJson["fields"];
            auto it = remove_if(fields.begin(), fields.end(), [&](const json& field) {
                return field["name"] == fieldName;
                });

            if (it != fields.end()) {
                fields.erase(it, fields.end());

                ofstream outFile(filePath);
                outFile << dbJson.dump(4);
                outFile.close();

                cout << "字段 " << fieldName << " 已成功从表 " << tableName << " 中删除。" << endl;
                return;
            }

            cerr << "未找到字段: " << fieldName << endl;
            return;
        }
    }

    cerr << "未找到表: " << tableName << endl;
}
//修改字段
void modifyFieldInTable(const string& tableName, const string& databaseName, const string& oldFieldName, const string& newFieldName, const string& newType) {
    string filePath = databaseName + ".json";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "无法打开数据库文件: " << filePath << endl;
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
                    outFile << dbJson.dump(4);
                    outFile.close();

                    cout << "字段 " << oldFieldName << " 已成功";
                    if (!newFieldName.empty()) {
                        cout << " 修改为 " << newFieldName;
                    }
                    if (!newType.empty()) {
                        cout << "，类型修改为 " << newType;
                    }
                    cout << endl;
                    return;
                }
            }

            cerr << "未找到字段: " << oldFieldName << endl;
            return;
        }
    }

    cerr << "未找到表: " << tableName << endl;
}


//建表封装
void handleCreateTable(const string& sql, map<string, vector<Table>>& databaseTables) {
    Table table = parseCreateTableStatement(sql);
    databaseTables[table.databaseName].push_back(table);
    table.print();
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
    }
}
//读表
void handleReadDatabase(const string& sql) {
    regex readRegex(R"(READ DATABASE (\w+);)", regex_constants::icase);
    smatch match;
    if (regex_match(sql, match, readRegex)) {
        string databaseName = match[1];
        readDatabaseFile(databaseName);
    }
    else {
        cerr << "READ DATABASE 语法错误：" << sql << endl;
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
    }
}
//修改字段名或类型封装
void handleModifyField(const string& sql) {
    // 正则表达式支持三种情况：
    // 1. 修改字段名和类型: MODIFY FIELD oldName TO newName TYPE newType IN tableName IN databaseName;
    // 2. 仅修改字段名: MODIFY FIELD oldName TO newName IN tableName IN databaseName;
    // 3. 仅修改字段类型: MODIFY FIELD oldName TYPE newType IN tableName IN databaseName;
    regex modifyRegexFull(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\w+);)", regex_constants::icase);
    regex modifyRegexNameOnly(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+IN\s+(\w+)\s+IN\s+(\w+);)", regex_constants::icase);
    regex modifyRegexTypeOnly(R"(MODIFY FIELD (\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\w+);)", regex_constants::icase);

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
    }
}

// 主函数：解析语句
int main() {
    map<string, vector<Table>> databaseTables;

    cout << "请输入 SQL 语句（输入 0 退出）：\n";
    while (true) {
        string sql;
        getline(cin, sql);
        if (sql == "0") {
            cout << "检测到输入 0，保存数据并退出。\n";
            saveTablesToDatabaseFiles(databaseTables); // 保存数据
            break;
        }

        try {
            if (sql.find("CREATE TABLE") == 0) {
                handleCreateTable(sql, databaseTables);
            }
            else if (sql.find("EDIT TABLE") == 0) {
                handleEditTable(sql);
            }
            else if (sql.find("RENAME TABLE") == 0) {
                handleRenameTable(sql);
            }
            else if (sql.find("DROP TABLE") == 0) {
                handleDropTable(sql);
            }
            else if (sql.find("READ DATABASE") == 0) {
                handleReadDatabase(sql);
            }
            else if (sql.find("ADD FIELD") == 0) {
                handleAddField(sql);
            }
            else if (sql.find("REMOVE FIELD") == 0) {
                handleRemoveField(sql);
            }
            else if (sql.find("MODIFY FIELD") == 0) {
                handleModifyField(sql);
            }
            else if (sql == "save") {
                cout << "检测到输入save，保存数据。\n";
                saveTablesToDatabaseFiles(databaseTables); // 保存数据
            }
            else {
                cerr << "不支持的操作：" << sql << endl;
            }
        }
        catch (const exception& e) {
            cerr << "错误: " << e.what() << endl;
        }
    }

    return 0;
}

