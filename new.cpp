#include <filesystem>

namespace fs = std::filesystem;

// 保存表信息到对应数据库的 JSON 文件//
void saveTablesToDatabaseFiles(const map<string, vector<Table>>& databaseTables) {
    for (const auto& [databaseName, tables] : databaseTables) {
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
        for (const auto& table : tables) {
            // 检测新表中字段的 keyFlag 和类型
            int keyCount = 0;
            for (const auto& field : table.fields) {
                if (field.keyFlag == "KEY") {
                    keyCount++;
                }

                // 检测字段类型是否合法
                const string& type = field.type;
                if (!regex_match(type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "表 " << table.tableName << " 中字段类型 " << type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    output << "表 " << table.tableName << " 中字段类型 " << type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    return;
                }
            }

            if (keyCount > 1) {
                cerr << "表 " << table.tableName << " 中存在多个字段标记为 KEY，不允许。" << endl;
                output << "表 " << table.tableName << " 中存在多个字段标记为 KEY，不允许。" << endl;
                return;
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
    }
}

// 编辑表字段//
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields) {
    string filePath = databaseName + ".dbf";
    string datFilePath = databaseName + "_" + tableName + ".dat";
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

    // 检查 .dat 文件是否有数据记录
    ifstream datFile(datFilePath);
    if (datFile.is_open()) {
        string line;
        getline(datFile, line); // 跳过表名
        getline(datFile, line); // 跳过记录数
        getline(datFile, line); // 跳过字段数量
        getline(datFile, line); // 跳过字段名
        if (getline(datFile, line)) { // 检查是否有数据记录
            cerr << "表 " << tableName << " 有数据记录，无法修改字段类型。" << endl;
            output << "表 " << tableName << " 有数据记录，无法修改字段类型。" << endl;
            datFile.close();
            return;
        }
        datFile.close();
    }

    // 查找表并编辑字段
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            found = true;

            // 更新字段：保留原字段，更新匹配字段，添加新字段
            auto& existingFields = tableJson["fields"];
            for (const auto& newField : newFields) {
                bool fieldExists = false;
                // 检测字段类型是否合法
                if (!regex_match(newField.type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "表中字段类型 " << newField.type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    output << "表中字段类型 " << newField.type << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                    return;
                }
                for (auto& existingField : existingFields) {
                    if (existingField["name"] == newField.name) {
                        // 更新现有字段的属性
                        existingField["type"] = newField.type;
                        existingField["key_flag"] = newField.keyFlag;
                        existingField["null_flag"] = newField.nullFlag;
                        existingField["valid_flag"] = newField.validFlag;
                        fieldExists = true;
                        break;
                    }
                }

                // 如果字段不存在，添加为新字段
                if (!fieldExists) {
                    json newFieldJson;
                    newFieldJson["name"] = newField.name;
                    newFieldJson["type"] = newField.type;
                    newFieldJson["key_flag"] = newField.keyFlag;
                    newFieldJson["null_flag"] = newField.nullFlag;
                    newFieldJson["valid_flag"] = newField.validFlag;
                    existingFields.push_back(newFieldJson);
                }
            }

            // 如果新字段中有主键，确保其他字段的主键标志被更新为非主键
            if (std::any_of(newFields.begin(), newFields.end(),
                [](const Field& field) { return field.keyFlag == "KEY"; })) {
                for (auto& existingField : existingFields) {
                    if (existingField["name"] != newFields[0].name) {
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

    // 写回文件
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // 使用 4 个空格缩进
        outFile.close();
        cout << "表 " << tableName << " 的字段已更新。\n";
        output << "表 " << tableName << " 的字段已更新。\n";
    }
    else {
        cerr << "无法打开文件 " << filePath << " 进行写入。" << endl;
        output << "无法打开文件 " << filePath << " 进行写入。" << endl;
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

// 从 JSON 文件加载数据库表信息
map<string, vector<Table>> loadDatabaseTables(const string& jsonFilePath) {
    ifstream inFile(jsonFilePath);
    if (!inFile.is_open()) {
        cerr << "无法打开文件: " << jsonFilePath << endl;
        output << "无法打开文件: " << jsonFilePath << endl;
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

    // 验证每个字段的类型和长度
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

    // 检查目标字段是否存在
    if (fieldIndexMap.find(targetField) == fieldIndexMap.end()) {
        cerr << "字段 " << targetField << " 不存在。" << endl;
        output << "字段 " << targetField << " 不存在。" << endl;
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

    // 构建字段名和类型的映射
    map<string, string> fieldTypes;
    for (const auto& field : table.fields) {
        fieldTypes[field.name] = field.type;
    }
    // 检查目标字段是否存在并提取类型
    if (fieldTypes.find(targetField) == fieldTypes.end()) {
        cerr << "字段 " << targetField << " 不存在于表 " << tableName << " 中。" << endl;
        return;
    }
    string fieldType = fieldTypes[targetField];

    // 验证目标字段值
    Field targetFieldInfo{ targetField, fieldType };
    if (validateFieldValue(targetFieldInfo, newValue) == false) {
        return;
    };

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
        Field newField = parseField(match[2]);
        addFieldToTable(tableName, databaseName, newField, databaseTables);
    }
    else {
        cerr << "ADD FIELD 语法错误：" << sql << endl;
        output << "ADD FIELD 语法错误：" << sql << endl;
    }
}

// 字段删除//
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
    if (!datFile.is_open()) {
        cerr << "无法打开表文件: " << datFilePath << endl;
        output << "无法打开表文件: " << datFilePath << endl;
        return;
    }

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

//修改字段//
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
            for (auto& field : tableJson["fields"]) {
                if (field["name"] == oldFieldName) {
                    // 检查新字段类型是否有效
                    if (!newType.empty() && !regex_match(newType, regex("(int|char|string)\\[\\d+\\]"))) {
                        cerr << "表 " << tableName << " 中字段类型 " << newType << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
                        output << "表 " << tableName << " 中字段类型 " << newType << " 无效，仅支持 int[x], char[x], string[x] 格式。" << endl;
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

                        // 直接读取但跳过最后一个字段
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
                            // 允许从 int 或 char 转换为 string
                            if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos) && newType.find("string") != string::npos) {
                                // 允许 int 或 char 转换为 string
                                // 不做任何处理，直接跳过
                            }
                            // 不允许从 string 转换为 int 或 char
                            else if (currentType.find("string") != string::npos && (newType.find("int") != string::npos || newType.find("char") != string::npos)) {
                                cerr << "不允许将字段类型从 string 转换为 " << newType << "。" << endl;
                                output << "不允许将字段类型从 string 转换为 " << newType << "。" << endl;
                                return;
                            }
                            // 允许同类型之间的大小变化：int -> int，char -> char
                            else if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos || currentType.find("string") != string::npos) &&
                                newType.find(currentType.substr(0, currentType.find('['))) != string::npos) {
                                // 允许 int[x] 转为 int[y] 或 char[x] 转为 char[y]
                                // 不做任何处理，直接跳过
                            }
                            else {
                                cerr << "不允许将字段类型从 " << currentType << " 转换为 " << newType << "。" << endl;
                                output << "不允许将字段类型从 " << currentType << " 转换为 " << newType << "。" << endl;
                                return;
                            }
                        }

                        // 更新第四行内容，避免多余的逗号
                        string updatedFields;
                        for (size_t i = 0; i < fields.size(); ++i) {
                            updatedFields += fields[i];
                            updatedFields += ",";
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
                }
            }

            return;
        }
    }

    cerr << "未找到表: " << tableName << endl;
    output << "未找到表: " << tableName << endl;
    // 调用汇总函数
    consolidateToDatabaseFile(databaseName, tables);
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

//函数处理//
void processSQLCommands(const string& sql, map<string, vector<Table>>& databaseTables) {
    try {
        if (containsIgnoreCase(sql, "CREATE TABLE")) {
            handleCreateTable(sql, databaseTables);
        }
        else if (containsIgnoreCase(sql, "EDIT TABLE")) {
            handleEditTable(sql);
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