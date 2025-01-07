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
    regex fieldRegex(R"((\w+)\s+(\w+\[\d+\]|\w+)\s+(KEY|NOT_KEY)\s+(NO_NULL|NULL)\s+(VALID))");
    smatch match;
    string trimmedField = trim(fieldLine);
    if (!regex_match(trimmedField, match, fieldRegex)) {
        throwError("Invalid field definition: " + fieldLine);
    }
    return { match[1], match[2], match[3], match[4], match[5] };
}

// 主函数：解析语句
Table parseCreateTableStatement(const string& sql) {
    Table table;
    regex mainRegex(R"(CREATE TABLE (\w+)\s*\((.+)\)\s*INTO (\w+);)");
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
//建表封装
void handleCreateTable(const string& sql, map<string, vector<Table>>& databaseTables) {
    Table table = parseCreateTableStatement(sql);
    databaseTables[table.databaseName].push_back(table);
    table.print();
}
//表修改封装
void handleEditTable(const string& sql) {
    regex editRegex(R"(EDIT TABLE (\w+)\s*\(\s*(.+?)\s*\)\s*IN\s+(\w+);)");
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
    regex renameRegex(R"(RENAME TABLE (\w+)\s+(\w+)\s+IN\s+(\w+);)");
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
    regex dropRegex(R"(DROP TABLE (\w+)\s+IN\s+(\w+);)");
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

