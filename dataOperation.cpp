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
}

void insertRecordToTableFile(const string& databaseName, const string& tableName, const vector<string>& values) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
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
}

void consolidateToDatabaseFile(const string& databaseName, const vector<Table>& tables) {
    string databaseFilePath = databaseName + ".dat";
    ofstream databaseFile(databaseFilePath, ios::binary);

    if (!databaseFile.is_open()) {
        cerr << "无法创建主数据库文件: " << databaseFilePath << endl;
        return;
    }

    databaseFile << "# " << databaseName << '\n'; // 写入数据库标识

    for (const auto& table : tables) {
        string tableFilePath = databaseName + "_" + table.tableName + ".dat";
        ifstream tableFile(tableFilePath, ios::binary);

        if (!tableFile.is_open()) {
            cerr << "无法打开表文件: " << tableFilePath << endl;
            continue;
        }

        databaseFile << tableFile.rdbuf(); // 直接追加表文件内容到数据库文件
        databaseFile.put('\n');            // 表之间添加换行分隔
        tableFile.close();
    }

    databaseFile.close();
    cout << "所有表已成功汇总到数据库文件 " << databaseFilePath << " 中。" << endl;
}

void insertRecord(const string& databaseName, const string& tableName, const vector<string>& values, map<string, vector<Table>>& databaseTables) {
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cerr << "数据库 " << databaseName << " 不存在。" << endl;
        return;
    }

    auto& tables = databaseTables[databaseName];
    auto it = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "表 " << tableName << " 不存在。" << endl;
        return;
    }

    Table& table = *it;

    if (values.size() != table.fieldCount) {
        cerr << "插入的字段数量不匹配，表 " << tableName << " 需要 " << table.fieldCount << " 个字段。" << endl;
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
}

void deleteRecordFromTableFile(const string& databaseName, const string& tableName, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
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
    }
    else {
        cerr << "未找到满足条件的记录，或记录已无效。" << endl;
    }
}

void updateRecordInTableFile(const string& databaseName, const string& tableName, const string& targetField, const string& newValue, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "表文件 " << datFilePath << " 不存在。" << endl;
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
        datFile.close();
        return;
    }

    // 检查条件字段是否存在
    for (const auto& condition : conditions) {
        if (fieldIndexMap.find(condition.first) == fieldIndexMap.end()) {
            cerr << "字段 " << condition.first << " 不存在。" << endl;
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
    }
    else {
        cerr << "未找到满足条件的记录，或记录无效。" << endl;
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
            databaseTables = loadDatabaseTables(databaseName + ".json"); // 假设 JSON 文件路径为 "MyDB.json"
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
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
            databaseTables = loadDatabaseTables(databaseName + ".json"); // 假设 JSON 文件路径为 "MyDB.json"
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
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
            return;
        }

        // 调用删除记录的函数
        deleteRecordFromTableFile(databaseName, tableName, conditions, databaseTables);
    }
    else {
        cerr << "DELETE FROM 语法错误：" << sql << endl;
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
            databaseTables = loadDatabaseTables(databaseName + ".json");
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
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
            return;
        }

        // 调用更新记录的函数
        updateRecordInTableFile(databaseName, tableName, targetField, newValue, conditions, databaseTables);
    }
    else {
        cerr << "UPDATE 语法错误：" << sql << endl;
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
            databaseTables = loadDatabaseTables(databaseName + ".json");
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "加载数据库失败或数据库不存在：" << databaseName << endl;
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
            return;
        }

        // 打开 .dat 文件并读取数据
        string datFilePath = databaseName + "_" + tableName + ".dat";
        ifstream datFile(datFilePath, ios::in | ios::binary);

        if (!datFile.is_open()) {
            cerr << "表文件 " << datFilePath << " 不存在。" << endl;
            return;
        }

        cout << "表 " << tableName << " 的有效记录：" << endl;
        // 跳过表名、记录数量和字段名行
        string line;
        getline(datFile, line); // 表名行
        getline(datFile, line); // 记录数量行
        getline(datFile, line); // 字段数量行
        getline(datFile, line); // 字段名行
        cout << line << endl;   // 输出字段名

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
                for (size_t i = 1; i < recordFields.size(); ++i) { // 跳过有效位字段
                    cout << recordFields[i];
                    if (i < recordFields.size() - 1) cout << ", ";
                }
                cout << endl;
            }
        }

        datFile.close();
    }
    else {
        cerr << "SHOW CONTENT 语法错误：" << sql << endl;
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