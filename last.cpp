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

//更新
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

//更新
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

//表修改封装////
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
            Field field = parseField(fieldLine);
            //
            if (!(field.type == "" && field.name == "" && field.keyFlag == "" && field.nullFlag == "" && field.validFlag == ""))
                newFields.push_back(field);
            else {
                cerr << "EDIT TABLE 参数错误：" << sql << endl;
                output << "EDIT TABLE 参数错误：" << sql << endl;
            }
        }

        editTableFields(tableName, databaseName, newFields, databaseTables);
    }
    else {
        cerr << "EDIT TABLE 语法错误：" << sql << endl;
        output << "EDIT TABLE 语法错误：" << sql << endl;
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

//函数处理////
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