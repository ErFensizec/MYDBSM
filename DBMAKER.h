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
#include <string>
#include <algorithm>
#include <cctype>
using namespace std;
using json = nlohmann::json;

#ifndef DBMAKER_H
#define DBMAKER_H

struct Field {
    string name;
    string type;
    string keyFlag;
    string nullFlag;
    string validFlag;
};
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
};




string trim(const string& str);
void throwError(const string& message);
void saveTablesToDatabaseFiles(const map<string, vector<Table>>& databaseTables);
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields);
void renameTable(const string& oldTableName, const string& newTableName, const string& databaseName) ;
void dropTable(const string& tableName, const string& databaseName);
void readDatabaseFile(const string& databaseName);
void addFieldToTable(const string& tableName, const string& databaseName, const Field& newField);
void removeFieldFromTable(const string& tableName, const string& databaseName, const string& fieldName);
void modifyFieldInTable(const string& tableName, const string& databaseName, const string& oldFieldName, const string& newFieldName, const string& newType);
void handleCreateTable(const string& sql, map<string, vector<Table>>& databaseTables);
void handleEditTable(const string& sql);
void handleRenameTable(const string& sql);
void handleDropTable(const string& sql);
void handleReadDatabase(const string& sql);
void handleAddField(const string& sql);
void handleRemoveField(const string& sql);
void handleModifyField(const string& sql);
void processSQLCommands(const string& sql, map<string, vector<Table>>& databaseTables);
// ×ª»»×Ö·û´®ÎªÐ¡Ð´
inline std::string toLowerCase(const std::string& str) {
    std::string lowerStr = str;
    std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);
    return lowerStr;
}

// ×ª»»×Ö·û´®Îª´óÐ´
inline std::string toUpperCase(const std::string& str) {
    std::string upperStr = str;
    std::transform(upperStr.begin(), upperStr.end(), upperStr.begin(), ::toupper);
    return upperStr;
}

// ÅÐ¶ÏÊÇ·ñ°üº¬×Ó×Ö·û´®£¨ºöÂÔ´óÐ¡Ð´£©
inline bool containsIgnoreCase(const std::string& text, const std::string& keyword) {
    std::string lowerText = toLowerCase(text);
    std::string lowerKeyword = toLowerCase(keyword);
    return lowerText.find(lowerKeyword) != std::string::npos;
}
void writeResult(string s);
void writeResult();
vector<string> streamParse(string s);
#endif // DBMAKER_H
