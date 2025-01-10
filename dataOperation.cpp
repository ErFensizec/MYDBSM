// �� JSON �ļ��������ݿ����Ϣ
map<string, vector<Table>> loadDatabaseTables(const string& jsonFilePath) {
    ifstream inFile(jsonFilePath);
    if (!inFile.is_open()) {
        cerr << "�޷����ļ�: " << jsonFilePath << endl;
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
        cerr << "�޷��������ļ�: " << datFilePath << endl;
        return;
    }

    datFile << "~" << table.tableName << '\n'; // д�����
    datFile << "0\n";                          // ��ʼ����¼����Ϊ 0
    datFile << table.fieldCount << '\n';       // д���ֶ�����

    datFile << "Valid" << ","; // "Valid" ����
    // д���ֶ���������
    for (const auto& field : table.fields) {
        datFile << field.name << ",";
    }

    datFile << '\n';
    datFile.close();
    cout << "�� " << table.tableName << " �� .dat �ļ������ɹ���" << endl;
}

void insertRecordToTableFile(const string& databaseName, const string& tableName, const vector<string>& values) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "���ļ� " << datFilePath << " �����ڡ�" << endl;
        return;
    }

    datFile.seekg(0, ios::beg);
    string line;
    int recordCount = 0;

    // ��ȡ��ǰ��¼����
    while (getline(datFile, line)) {
        if (line.find_first_not_of("0123456789") == string::npos) { // ����Ƿ�Ϊ��¼������
            recordCount = stoi(line);
            recordCount++;
            break;
        }
    }

    // ���¼�¼����
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

    // ��λ���ļ�ĩβд���¼�¼
    datFile.clear();
    datFile.seekp(0, ios::end);
    datFile << "1,"; // ��Ч��־�ͻ��з�
    for (size_t i = 0; i < values.size(); ++i) {
        datFile << left << values[i] << ",";

    }
    datFile << '\n';

    datFile.close();
    cout << "��¼���뵽�� " << tableName << " �ɹ���" << endl;
}

void consolidateToDatabaseFile(const string& databaseName, const vector<Table>& tables) {
    string databaseFilePath = databaseName + ".dat";
    ofstream databaseFile(databaseFilePath, ios::binary);

    if (!databaseFile.is_open()) {
        cerr << "�޷����������ݿ��ļ�: " << databaseFilePath << endl;
        return;
    }

    databaseFile << "# " << databaseName << '\n'; // д�����ݿ��ʶ

    for (const auto& table : tables) {
        string tableFilePath = databaseName + "_" + table.tableName + ".dat";
        ifstream tableFile(tableFilePath, ios::binary);

        if (!tableFile.is_open()) {
            cerr << "�޷��򿪱��ļ�: " << tableFilePath << endl;
            continue;
        }

        databaseFile << tableFile.rdbuf(); // ֱ��׷�ӱ��ļ����ݵ����ݿ��ļ�
        databaseFile.put('\n');            // ��֮����ӻ��зָ�
        tableFile.close();
    }

    databaseFile.close();
    cout << "���б��ѳɹ����ܵ����ݿ��ļ� " << databaseFilePath << " �С�" << endl;
}

void insertRecord(const string& databaseName, const string& tableName, const vector<string>& values, map<string, vector<Table>>& databaseTables) {
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cerr << "���ݿ� " << databaseName << " �����ڡ�" << endl;
        return;
    }

    auto& tables = databaseTables[databaseName];
    auto it = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "�� " << tableName << " �����ڡ�" << endl;
        return;
    }

    Table& table = *it;

    if (values.size() != table.fieldCount) {
        cerr << "������ֶ�������ƥ�䣬�� " << tableName << " ��Ҫ " << table.fieldCount << " ���ֶΡ�" << endl;
        return;
    }

    // ������ļ������ڣ��򴴽��±��ļ�
    string tableFilePath = databaseName + "_" + tableName + ".dat";
    ifstream checkFile(tableFilePath, ios::binary);
    if (!checkFile.is_open()) {
        createTableDatFile(databaseName, table);
    }
    checkFile.close();

    // �����¼�����ļ�
    insertRecordToTableFile(databaseName, tableName, values);
    consolidateToDatabaseFile(databaseName, tables);
    cout << "��¼�Ѳ���� " << tableName << "�������������ݿ��ļ�" << databaseName << endl;
}

void deleteRecordFromTableFile(const string& databaseName, const string& tableName, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "���ļ� " << datFilePath << " �����ڡ�" << endl;
        return;
    }

    datFile.seekg(0, ios::beg);
    string line;
    vector<string> fieldNames;
    map<string, int> fieldIndexMap; // �ֶ�����������ӳ��
    int fieldCount = 0;
    int recordCount = 0;
    int sum1 = 0;
    // ���������ͼ�¼������
    getline(datFile, line); // ������
    getline(datFile, line); // ��¼������
    recordCount = stoi(line);
    getline(datFile, line); // �ֶ�������
    fieldCount = stoi(line);

    // �����ֶ�����
    if (getline(datFile, line)) {
        stringstream ss(line);
        string field;
        int index = 0;
        while (getline(ss, field, ',')) {
            field = trim(field); // ȥ������ո�
            fieldNames.push_back(field);
            fieldIndexMap[field] = index++;
        }
    }

    // ��������ֶ��Ƿ����
    for (const auto& condition : conditions) {
        if (fieldIndexMap.find(condition.first) == fieldIndexMap.end()) {
            cerr << "�ֶ� " << condition.first << " �����ڡ�" << endl;
            datFile.close();
            return;
        }
    }

    bool found = false;
    vector<string> updatedLines;
    int sum = 0;
    // ������¼������ɨ��
    while (getline(datFile, line)) {
        stringstream ss(line);
        vector<string> recordFields;
        string fieldValue;

        // �����ŷָ�������¼
        while (getline(ss, fieldValue, ',')) {
            recordFields.push_back(trim(fieldValue)); // ȥ������ո�
        }

        // ����Ƿ�������������
        bool matches = true;
        for (const auto& condition : conditions) {
            int index = fieldIndexMap[condition.first];
            if (recordFields[index] != condition.second) {
                matches = false;
                break;
            }
        }

        // ������������Ҽ�¼��Ч������Ϊ��Ч
        if (matches && recordFields[0] == "1") {
            found = true;
            recordFields[0] = "0"; // ����Чλ��Ϊ��Ч
            sum1 = sum1 + 1;
        }

        // ������µļ�¼��
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
        // д���ļ�
        ofstream outFile(datFilePath, ios::out | ios::trunc | ios::binary);
        if (!outFile.is_open()) {
            cerr << "�޷����ļ�����д�ز�����" << endl;
            return;
        }

        // д�ر�������¼�������ֶ�����
        outFile << "~" << tableName << '\n';
        outFile << recordCount - sum1 << '\n'; // ���¼�¼����
        outFile << fieldCount << '\n';      // д���ֶ�����
        for (const auto& field : fieldNames) {
            outFile << field << ",";
        }
        outFile << '\n';

        // д�ظ��º�ļ�¼
        for (const auto& updatedLine : updatedLines) {
            outFile << updatedLine << "," << '\n';
        }
        outFile.close();

        // ���ܵ����ݿ��ļ�
        auto& tables = databaseTables[databaseName];
        consolidateToDatabaseFile(databaseName, tables);
        cout << "��¼ɾ���ɹ������Ϊ��Ч����" << endl;
    }
    else {
        cerr << "δ�ҵ����������ļ�¼�����¼����Ч��" << endl;
    }
}

void updateRecordInTableFile(const string& databaseName, const string& tableName, const string& targetField, const string& newValue, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "���ļ� " << datFilePath << " �����ڡ�" << endl;
        return;
    }

    datFile.seekg(0, ios::beg);
    string line;
    vector<string> fieldNames;
    map<string, int> fieldIndexMap; // �ֶ�����������ӳ��
    int fieldCount = 0;
    int recordCount = 0;

    // ���������ͼ�¼������
    getline(datFile, line); // ������
    getline(datFile, line); // ��¼������
    recordCount = stoi(line);
    getline(datFile, line); // �ֶ�������
    fieldCount = stoi(line);

    // �����ֶ�����
    if (getline(datFile, line)) {
        stringstream ss(line);
        string field;
        int index = 0;
        while (getline(ss, field, ',')) {
            field = trim(field); // ȥ������ո�
            fieldNames.push_back(field);
            fieldIndexMap[field] = index++;
        }
    }

    // ���Ŀ���ֶ��Ƿ����
    if (fieldIndexMap.find(targetField) == fieldIndexMap.end()) {
        cerr << "�ֶ� " << targetField << " �����ڡ�" << endl;
        datFile.close();
        return;
    }

    // ��������ֶ��Ƿ����
    for (const auto& condition : conditions) {
        if (fieldIndexMap.find(condition.first) == fieldIndexMap.end()) {
            cerr << "�ֶ� " << condition.first << " �����ڡ�" << endl;
            datFile.close();
            return;
        }
    }

    bool updated = false;
    vector<string> updatedLines;

    // ������¼������ɨ��
    while (getline(datFile, line)) {
        stringstream ss(line);
        vector<string> recordFields;
        string fieldValue;

        // �����ŷָ�������¼
        while (getline(ss, fieldValue, ',')) {
            recordFields.push_back(trim(fieldValue)); // ȥ������ո�
        }

        // ����Ƿ�������������
        bool matches = true;
        for (const auto& condition : conditions) {
            int index = fieldIndexMap[condition.first];
            if (recordFields[index] != condition.second) {
                matches = false;
                break;
            }
        }
        // ������������Ҽ�¼��Ч�������Ŀ���ֶ�
        if (matches && recordFields[0] == "1") {
            int targetIndex = fieldIndexMap[targetField];
            recordFields[targetIndex] = newValue; // �����ֶ�ֵ
            updated = true;
        }

        // ������µļ�¼��
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
        // д���ļ�
        ofstream outFile(datFilePath, ios::out | ios::trunc | ios::binary);
        if (!outFile.is_open()) {
            cerr << "�޷����ļ�����д�ز�����" << endl;
            return;
        }

        // д�ر�������¼�������ֶ�����
        outFile << "~" << tableName << '\n';
        outFile << recordCount << '\n';  // ��¼�������ֲ���
        outFile << fieldCount << '\n';  // �ֶ��������ֲ���
        for (const auto& field : fieldNames) {
            outFile << field << ",";
        }
        outFile << '\n';

        // д�ظ��º�ļ�¼
        for (const auto& updatedLine : updatedLines) {
            outFile << updatedLine << "," << '\n';
        }
        outFile.close();

        // ���ܵ����ݿ��ļ�
        auto& tables = databaseTables[databaseName];
        consolidateToDatabaseFile(databaseName, tables);
        cout << "��¼���³ɹ���" << endl;
    }
    else {
        cerr << "δ�ҵ����������ļ�¼�����¼��Ч��" << endl;
    }
}

void handleInsertRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // ���� INSERT INTO ��������ʽ
    regex insertRegex(R"(INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(([^)]+)\)\s+IN\s+(\w+)\s*;)", regex_constants::icase);
    smatch match;

    // ��� SQL ����Ƿ�ƥ��
    if (regex_match(sql, match, insertRegex)) {
        string tableName = match[1]; // ����
        string valuesStr = match[2]; // �ֶ�ֵ
        string databaseName = match[3]; // ���ݿ���

        // ����Ƿ���Ҫ�������ݿ�
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".json"); // ���� JSON �ļ�·��Ϊ "MyDB.json"
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
                return;
            }
        }
        // ���ֶ�ֵ�ָ�� vector
        vector<string> values;
        stringstream ss(valuesStr);
        string value;
        while (getline(ss, value, ',')) {
            value.erase(0, value.find_first_not_of(" "));
            value.erase(value.find_last_not_of(" ") + 1);
            values.push_back(value);
        }

        // ���ò����¼�ĺ���
        insertRecord(databaseName, tableName, values, databaseTables);
    }
    else {
        cerr << "INSERT INTO �﷨����" << sql << endl;
    }
}

void handleDeleteRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // ���� DELETE FROM ��������ʽ��֧�ֶ������
    regex deleteRegex(R"(DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+?)\s+IN\s+(\w+)\s*;)", regex_constants::icase);
    smatch match;

    // ��� SQL ����Ƿ�ƥ��
    if (regex_match(sql, match, deleteRegex)) {
        string tableName = match[1];     // ����
        string whereClause = match[2];  // WHERE �Ӿ�
        string databaseName = match[3]; // ���ݿ���

        // ����Ƿ���Ҫ�������ݿ�
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".json"); // ���� JSON �ļ�·��Ϊ "MyDB.json"
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
                return;
            }
        }

        // ���� WHERE �Ӿ�Ϊ��������
        vector<pair<string, string>> conditions;
        regex conditionRegex(R"((\w+)=([^\s]+))", regex_constants::icase);
        auto whereBegin = sregex_iterator(whereClause.begin(), whereClause.end(), conditionRegex);
        auto whereEnd = sregex_iterator();

        for (auto it = whereBegin; it != whereEnd; ++it) {
            conditions.emplace_back((*it)[1], (*it)[2]); // �ֶ�����ֵ
        }

        if (conditions.empty()) {
            cerr << "WHERE �Ӿ����ʧ�ܻ�Ϊ�գ�" << whereClause << endl;
            return;
        }

        // ����ɾ����¼�ĺ���
        deleteRecordFromTableFile(databaseName, tableName, conditions, databaseTables);
    }
    else {
        cerr << "DELETE FROM �﷨����" << sql << endl;
    }
}

void handleUpdateRecord(const string& sql, map<string, vector<Table>>& databaseTables) {
    // ���� UPDATE ��������ʽ��֧�ֶ������������
    regex updateRegex(
        R"(UPDATE\s+(\w+)\s*\(\s*SET\s+(\w+)\s*=\s*([^)]+)\s+WHERE\s+(.+?)\s*\)\s+IN\s+(\w+)\s*;)",
        regex_constants::icase
    );
    smatch match;

    // ��� SQL ����Ƿ�ƥ��
    if (regex_match(sql, match, updateRegex)) {
        string tableName = match[1];        // ����
        string targetField = match[2];     // Ҫ�޸ĵ��ֶ�
        string newValue = trim(match[3]);  // ��ֵ
        string whereClause = match[4];     // WHERE �Ӿ�
        string databaseName = match[5];    // ���ݿ���

        // ����Ƿ���Ҫ�������ݿ�
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".json");
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
                return;
            }
        }

        // ���� WHERE �Ӿ�Ϊ��������
        vector<pair<string, string>> conditions;
        regex conditionRegex(R"((\w+)\s*=\s*([^\s]+))", regex_constants::icase);
        auto whereBegin = sregex_iterator(whereClause.begin(), whereClause.end(), conditionRegex);
        auto whereEnd = sregex_iterator();

        for (auto it = whereBegin; it != whereEnd; ++it) {
            conditions.emplace_back((*it)[1], (*it)[2]); // �ֶ�����ֵ
        }

        if (conditions.empty()) {
            cerr << "WHERE �Ӿ����ʧ�ܻ�Ϊ�գ�" << whereClause << endl;
            return;
        }

        // ���ø��¼�¼�ĺ���
        updateRecordInTableFile(databaseName, tableName, targetField, newValue, conditions, databaseTables);
    }
    else {
        cerr << "UPDATE �﷨����" << sql << endl;
    }
}

void handleShowContent(const string& sql, map<string, vector<Table>>& databaseTables) {
    // ���� SHOW CONTENT ��������ʽ
    regex showRegex(R"(SHOW\s+CONTENT\s+(\w+)\s+IN\s+(\w+)\s*;)", regex_constants::icase);
    smatch match;

    // ��� SQL ����Ƿ�ƥ��
    if (regex_match(sql, match, showRegex)) {
        string tableName = match[1];      // ����
        string databaseName = match[2];  // ���ݿ���

        // ����Ƿ���Ҫ�������ݿ�
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
            databaseTables = loadDatabaseTables(databaseName + ".json");
            if (databaseTables.find(databaseName) == databaseTables.end()) {
                cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
                return;
            }
        }

        // �����ݿ��в��ұ�
        auto& tables = databaseTables[databaseName];
        auto tableIt = find_if(tables.begin(), tables.end(), [&](const Table& t) {
            return t.tableName == tableName;
            });

        if (tableIt == tables.end()) {
            cerr << "�� " << tableName << " �����������ݿ� " << databaseName << " �С�" << endl;
            return;
        }

        // �� .dat �ļ�����ȡ����
        string datFilePath = databaseName + "_" + tableName + ".dat";
        ifstream datFile(datFilePath, ios::in | ios::binary);

        if (!datFile.is_open()) {
            cerr << "���ļ� " << datFilePath << " �����ڡ�" << endl;
            return;
        }

        cout << "�� " << tableName << " ����Ч��¼��" << endl;
        // ������������¼�������ֶ�����
        string line;
        getline(datFile, line); // ������
        getline(datFile, line); // ��¼������
        getline(datFile, line); // �ֶ�������
        getline(datFile, line); // �ֶ�����
        cout << line << endl;   // ����ֶ���

        // ��ȡ��չʾ������Ч��¼
        while (getline(datFile, line)) {
            if (line.empty()) continue;
            vector<string> recordFields;
            stringstream ss(line);
            string fieldValue;

            // �����ŷָ���¼�ֶ�
            while (getline(ss, fieldValue, ',')) {
                recordFields.push_back(trim(fieldValue)); // ȥ������ո�
            }

            // �����Чλ
            if (!recordFields.empty() && recordFields[0] == "1") { // "1" ��ʾ��Ч
                for (size_t i = 1; i < recordFields.size(); ++i) { // ������Чλ�ֶ�
                    cout << recordFields[i];
                    if (i < recordFields.size() - 1) cout << ", ";
                }
                cout << endl;
            }
        }

        datFile.close();
    }
    else {
        cerr << "SHOW CONTENT �﷨����" << sql << endl;
    }
}

//��������
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
            cout << "��⵽����save���������ݡ�\n";
            saveTablesToDatabaseFiles(databaseTables); // ��������
        }
        else {
            cerr << "��֧�ֵĲ�����" << sql << endl;
        }
    }
    catch (const exception& e) {
        cerr << "����: " << e.what() << endl;
    }
}