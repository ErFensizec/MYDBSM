//new
bool isPrimaryKeyDuplicate(const string& databaseName, const string& tableName, size_t primaryKeyIndex, const string& primaryKeyValue) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    ifstream datFile(datFilePath, ios::binary);

    if (!datFile.is_open()) {
        cerr << "���ļ� " << datFilePath << " �����ڡ�" << endl;
        output << "���ļ� " << datFilePath << " �����ڡ�" << endl;
        return false;
    }

    string line;
    while (getline(datFile, line)) {
        if (line.empty() || line[0] != '1') { // ������Ч��¼
            continue;
        }

        istringstream recordStream(line);
        vector<string> fields;
        string field;

        while (getline(recordStream, field, ',')) {
            fields.push_back(field);
        }

        if (primaryKeyIndex < fields.size() && fields[primaryKeyIndex] == primaryKeyValue) {
            return true; // �����ֶ�ֵ�Ѵ���
        }
    }

    return false;
}

//����
void insertRecord(const string& databaseName, const string& tableName, const vector<string>& values, map<string, vector<Table>>& databaseTables) {
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cerr << "���ݿ� " << databaseName << " �����ڡ�" << endl;
        output << "���ݿ� " << databaseName << " �����ڡ�" << endl;
        return;
    }

    auto& tables = databaseTables[databaseName];
    auto it = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "�� " << tableName << " �����ڡ�" << endl;
        output << "�� " << tableName << " �����ڡ�" << endl;
        return;
    }

    Table& table = *it;

    if (values.size() != table.fieldCount) {
        cerr << "������ֶ�������ƥ�䣬�� " << tableName << " ��Ҫ " << table.fieldCount << " ���ֶΡ�" << endl;
        output << "������ֶ�������ƥ�䣬�� " << tableName << " ��Ҫ " << table.fieldCount << " ���ֶΡ�" << endl;
        return;
    }

    // ��֤�����ֶ��Ƿ��ظ�
    for (size_t i = 0; i < table.fields.size(); ++i) {
        const auto& field = table.fields[i];
        if (field.keyFlag == "KEY") {
            if (isPrimaryKeyDuplicate(databaseName, tableName, i + 1, values[i])) {
                cerr << "�ֶ� " << field.name << " ��ֵ \"" << values[i] << "\" �Ѵ��ڣ�Υ������Լ����" << endl;
                output << "�ֶ� " << field.name << " ��ֵ \"" << values[i] << "\" �Ѵ��ڣ�Υ������Լ����" << endl;
                return;
            }
        }
    }

    // ��֤�ֶ����ͺͳ���
    for (size_t i = 0; i < table.fields.size(); ++i) {
        const auto& field = table.fields[i];
        const auto& value = values[i];

        // ��ȡ�ֶ����ͺͳ���
        regex typeRegex(R"((int|char|string)\[(\d+)\])", regex_constants::icase);
        smatch match;

        if (regex_match(field.type, match, typeRegex)) {
            string type = match[1];
            int length = stoi(match[2]);

            // ��֤�ֶγ���
            if (value.length() > length) {
                cerr << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ���ȳ������� (" << length << ")��" << endl;
                output << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ���ȳ������� (" << length << ")��" << endl;
                return;
            }

            // ��֤�ֶ�����
            if (type == "int") {
                if (!regex_match(value, regex(R"(\d+)"))) {
                    cerr << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ int ����Ҫ��" << endl;
                    output << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ int ����Ҫ��" << endl;
                    return;
                }
            }
            else if (type == "char") {
                if (!regex_match(value, regex(R"([a-zA-Z]+)"))) {
                    cerr << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ char ����Ҫ��" << endl;
                    output << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ char ����Ҫ��" << endl;
                    return;
                }
            }
            // string �������������֤
        }
        else {
            cerr << "�ֶ� " << field.name << " ������ \"" << field.type << "\" ��Ч��" << endl;
            output << "�ֶ� " << field.name << " ������ \"" << field.type << "\" ��Ч��" << endl;
            return;
        }
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
    cout << "��¼�Ѳ���� " << tableName << "�������������ݿ��ļ� " << databaseName << endl;
    output << "��¼�Ѳ���� " << tableName << "�������������ݿ��ļ� " << databaseName << endl;
}

//����
void updateRecordInTableFile(const string& databaseName, const string& tableName, const string& targetField, const string& newValue, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in | ios::out | ios::binary);

    if (!datFile.is_open()) {
        cerr << "���ļ� " << datFilePath << " �����ڡ�" << endl;
        output << "���ļ� " << datFilePath << " �����ڡ�" << endl;
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

    // �����ֶ�����������
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

    // ȷ������������ݿ���
    auto it = databaseTables.find(databaseName);
    if (it == databaseTables.end()) {
        cerr << "���ݿ� " << databaseName << " �����ڡ�" << endl;
        return;
    }

    auto& tables = it->second;
    auto tableIt = find_if(tables.begin(), tables.end(), [&tableName](const Table& table) {
        return table.tableName == tableName;
        });

    if (tableIt == tables.end()) {
        cerr << "�� " << tableName << " �����������ݿ� " << databaseName << " �С�" << endl;
        return;
    }

    Table& table = *tableIt;

    // ���Ŀ���ֶ��Ƿ�Ϊ����
    bool isPrimaryKey = false;
    size_t primaryKeyIndex = 0; // ��������
    for (const auto& field : table.fields) {
        if (field.name == targetField) {
            if (field.keyFlag == "KEY") {
                isPrimaryKey = true;
            }
            primaryKeyIndex = fieldIndexMap[targetField];
            break;
        }
    }

    // ���Ŀ���ֶ��������������ֵ�Ƿ��ظ�
    if (isPrimaryKey && isPrimaryKeyDuplicate(databaseName, tableName, primaryKeyIndex, newValue)) {
        cerr << "����ʧ�ܣ������ֶ� " << targetField << " ��ֵ \"" << newValue << "\" �Ѵ��ڡ�" << endl;
        output << "����ʧ�ܣ������ֶ� " << targetField << " ��ֵ \"" << newValue << "\" �Ѵ��ڡ�" << endl;
        datFile.close();
        return;
    }

    // �����߼����ֲ���
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
            output << "�޷����ļ�����д�ز�����" << endl;
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
        output << "��¼���³ɹ���" << endl;
    }
    else {
        cerr << "δ�ҵ����������ļ�¼�����¼��Ч��" << endl;
        output << "δ�ҵ����������ļ�¼�����¼��Ч��" << endl;
    }
}

// �ֶ�ɾ��////
void removeFieldFromTable(const string& tableName, const string& databaseName, const string& fieldName, map<string, vector<Table>>& databaseTables) {
    // ȷ�����ݿ��Ѽ���
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        output << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            output << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            return;
        }
    }

    // ��ȡ�����
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "δ�ҵ���: " << tableName << endl;
        output << "δ�ҵ���: " << tableName << endl;
        return;
    }

    Table& table = *it;
    auto fieldIt = std::find_if(table.fields.begin(), table.fields.end(), [&](const Field& field) {
        return field.name == fieldName;
        });

    if (fieldIt == table.fields.end()) {
        cerr << "δ�ҵ��ֶ�: " << fieldName << endl;
        output << "δ�ҵ��ֶ�: " << fieldName << endl;
        return;
    }

    // �ҵ��ֶ�����
    int fieldIndex = std::distance(table.fields.begin(), fieldIt);

    // ɾ���ֶ�Ԫ��Ϣ
    table.fields.erase(fieldIt);
    table.fieldCount = table.fields.size();

    string datFilePath = databaseName + "_" + tableName + ".dat";
    fstream datFile(datFilePath, ios::in);
    if (datFile.is_open()) {

        // ��ȡ�ļ�����
        vector<string> fileLines;
        string line;
        while (getline(datFile, line)) {
            fileLines.push_back(line);
        }
        datFile.close();

        // �޸ĵ������ֶ���
        if (fileLines.size() < 4) {
            cerr << "�ļ��������� 4 �У��޷���ɲ�����" << endl;
            output << "�ļ��������� 4 �У��޷���ɲ�����" << endl;
            return;
        }
        fileLines[2] = to_string(table.fieldCount);

        // �޸ĵ������ֶ�����
        string newHeader = "Valid,";
        for (const auto& field : table.fields) {
            newHeader += field.name + ",";
        }
        fileLines[3] = newHeader;

        // ɾ���������ж�Ӧ�ֶ�
        for (size_t i = 4; i < fileLines.size(); ++i) {
            stringstream ss(fileLines[i]);
            vector<string> cells;
            string cell;
            while (getline(ss, cell, ',')) {
                cells.push_back(cell);
            }

            // ɾ��Ŀ���ֶ�λ��
            if (fieldIndex + 1 < cells.size()) {
                cells.erase(cells.begin() + fieldIndex + 1);
            }

            // ���¹�����
            string updatedLine;
            for (const auto& c : cells) {
                updatedLine += c + ",";
            }
            fileLines[i] = updatedLine;
        }

        // д���޸ĺ���ļ�
        ofstream outFile(datFilePath, ios::trunc | ios::binary);
        if (!outFile.is_open()) {
            cerr << "�޷�����д����ļ�: " << datFilePath << endl;
            output << "�޷�����д����ļ�: " << datFilePath << endl;
            return;
        }
        for (const auto& updatedLine : fileLines) {
            outFile << updatedLine << '\n';
        }
        outFile.close();
    }
    // �������ݿ�Ԫ��Ϣ�ļ�
    string dbfFilePath = databaseName + ".dbf";
    ifstream dbfFile(dbfFilePath);
    if (!dbfFile.is_open()) {
        cerr << "�޷������ݿ�Ԫ��Ϣ�ļ�: " << dbfFilePath << endl;
        output << "�޷������ݿ�Ԫ��Ϣ�ļ�: " << dbfFilePath << endl;
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
        cout << "�ֶ� " << fieldName << " ɾ���ɹ�����ǰ�ֶ���: " << table.fieldCount << endl;
        output << "�ֶ� " << fieldName << " ɾ���ɹ�����ǰ�ֶ���: " << table.fieldCount << endl;
    }
    // ���û��ܺ���
    consolidateToDatabaseFile(databaseName, tables);
}

// �༭���ֶ�//?���ܣ��������������
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields, map<string, vector<Table>>& databaseTables) {
    string filePath = databaseName + ".dbf";
    string datFilePath = databaseName + "_" + tableName + ".dat";

    // ��ȡ���ݿ��ļ�
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "�޷������ݿ��ļ�: " << filePath << endl;
        output << "�޷������ݿ��ļ�: " << filePath << endl;
        return;
    }

    json dbJson;
    try {
        inFile >> dbJson;
    }
    catch (const exception& e) {
        cerr << "��ȡ���ݿ��ļ�ʱ����: " << e.what() << endl;
        output << "��ȡ���ݿ��ļ�ʱ����: " << e.what() << endl;
        inFile.close();
        return;
    }
    inFile.close();

    // ����Ƿ���Ҫ�������ݿ⵽�ڴ�
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        output << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            output << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            return;
        }
    }

    // ��ȡ�ڴ��еı����
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "δ�ҵ���: " << tableName << endl;
        output << "δ�ҵ���: " << tableName << endl;
        return;
    }

    Table& table = *it;

    // ��� .dat �ļ��Ƿ������ݼ�¼
    ifstream datFile(datFilePath);
    if (datFile.is_open()) {
        string line;
        for (int i = 0; i < 4; ++i) getline(datFile, line); // ����ǰ 4 ��
        if (getline(datFile, line)) {
            cerr << "�� " << tableName << " �����ݼ�¼���޷��޸��ֶ����͡�" << endl;
            output << "�� " << tableName << " �����ݼ�¼���޷��޸��ֶ����͡�" << endl;
            datFile.close();
            return;
        }
        datFile.close();
    }

    // ����Ŀ���
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            found = true;

            // �����ֶΣ���һƥ�䲢�����ֶ���Ϣ
            auto& existingFields = tableJson["fields"];
            for (const auto& newField : newFields) {
                bool fieldExists = false;
                if (!regex_match(newField.type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "�ֶ����� " << newField.type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                    output << "�ֶ����� " << newField.type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
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

            // ȷ������Ψһ��
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
        cerr << "δ�ҵ��� " << tableName << "��" << endl;
        output << "δ�ҵ��� " << tableName << "��" << endl;
        return;
    }

    // �����޸ĺ�� JSON ����
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4);
        outFile.close();
        cout << "�� " << tableName << " ���ֶ��Ѹ��¡�" << endl;
        output << "�� " << tableName << " ���ֶ��Ѹ��¡�" << endl;
    }
    else {
        cerr << "�޷����ļ� " << filePath << " ����д�롣" << endl;
        output << "�޷����ļ� " << filePath << " ����д�롣" << endl;
    }

    // ���û��ܺ���
    tables = databaseTables[databaseName];
    consolidateToDatabaseFile(databaseName, tables);
}

//���޸ķ�װ////
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
                cerr << "EDIT TABLE ��������" << sql << endl;
                output << "EDIT TABLE ��������" << sql << endl;
            }
        }

        editTableFields(tableName, databaseName, newFields, databaseTables);
    }
    else {
        cerr << "EDIT TABLE �﷨����" << sql << endl;
        output << "EDIT TABLE �﷨����" << sql << endl;
    }
}
//�޸��ֶ�//?�������������//
void modifyFieldInTable(const string& tableName, const string& databaseName, const string& oldFieldName, const string& newFieldName, const string& newType, map<string, vector<Table>>& databaseTables) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "�޷������ݿ��ļ�: " << filePath << endl;
        output << "�޷������ݿ��ļ�: " << filePath << endl;
        return;
    }

    json dbJson;
    try {
        inFile >> dbJson;
    }
    catch (const exception& e) {
        cerr << "��ȡ���ݿ��ļ�ʱ����: " << e.what() << endl;
        output << "��ȡ���ݿ��ļ�ʱ����: " << e.what() << endl;
        inFile.close();
        return;
    }
    inFile.close();
    // ����Ƿ���Ҫ�������ݿ⵽�ڴ�
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        output << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf");
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            output << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            return;
        }
    }

    // ��ȡ�ڴ��еı����
    auto& tables = databaseTables[databaseName];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table) {
        return table.tableName == tableName;
        });

    if (it == tables.end()) {
        cerr << "δ�ҵ���: " << tableName << endl;
        output << "δ�ҵ���: " << tableName << endl;
        return;
    }

    Table& table = *it;
    // ����Ŀ���
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            // ����ֶ�ֵ�Ƿ��ظ�
            for (const auto& field : tableJson["fields"]) {
                if (field["name"] == newFieldName && field["name"] != oldFieldName) {
                    cerr << "�� " << tableName << " ���Ѵ����ֶ�����: " << newFieldName << endl;
                    output << "�� " << tableName << " ���Ѵ����ֶ�����: " << newFieldName << endl;
                    return;
                }
            }

            // ��ȡ `dat` �ļ�·��
            string datFilePath = databaseName + "_" + tableName + ".dat";
            fstream datFile(datFilePath, ios::in | ios::out);
            bool datFileExists = datFile.is_open();

            // ����Ŀ���ֶβ��޸�
            bool fieldFound = false; // ��־λ
            for (auto& field : tableJson["fields"]) {
                if (field["name"] == oldFieldName) {
                    fieldFound = true;

                    // ������ֶ������Ƿ���Ч
                    if (!newType.empty() && !regex_match(newType, regex("(int|char|string)\\[\\d+\\]"))) {
                        cerr << "�� " << tableName << " ���ֶ����� " << newType << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                        output << "�� " << tableName << " ���ֶ����� " << newType << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                        return;
                    }
                    if (datFileExists && !newType.empty() && newType != field["type"].get<string>()) {
                        cerr << "���������ļ�����ֹ�޸��ֶ�����: " << oldFieldName << endl;
                        output << "���������ļ�����ֹ�޸��ֶ�����: " << oldFieldName << endl;
                        return;
                    }
                    // �����ֶ���Ϣ�� JSON ����
                    if (!newFieldName.empty()) {
                        field["name"] = newFieldName;
                    }
                    if (!newType.empty()) {
                        field["type"] = newType;
                    }

                    // �����޸ĺ�� JSON ����
                    ofstream outFile(filePath);
                    if (outFile.is_open()) {
                        outFile << dbJson.dump(4);
                        outFile.close();
                    }
                    else {
                        cerr << "�޷����ļ� " << filePath << " ����д�롣" << endl;
                        output << "�޷����ļ� " << filePath << " ����д�롣" << endl;
                        return;
                    }

                    // ��� `dat` �ļ����ڣ������ֶ��������͵�Լ��
                    if (datFileExists) {
                        vector<string> fileLines;
                        string line;
                        while (getline(datFile, line)) {
                            fileLines.push_back(line);
                        }

                        if (fileLines.size() < 4) {
                            cerr << "���ļ���ʽ����ȷ���������� 4 �С�" << endl;
                            output << "���ļ���ʽ����ȷ���������� 4 �С�" << endl;
                            datFile.close();
                            return;
                        }

                        // �����������ֶ���
                        stringstream ss(fileLines[3]);
                        string cell;
                        vector<string> fields;
                        while (getline(ss, cell, ',')) {
                            fields.push_back(cell);
                        }

                        auto it = find(fields.begin(), fields.end(), oldFieldName);
                        if (it == fields.end()) {
                            cerr << "�ڱ��ļ���δ�ҵ��ֶ�����: " << oldFieldName << endl;
                            output << "�ڱ��ļ���δ�ҵ��ֶ�����: " << oldFieldName << endl;
                            datFile.close();
                            return;
                        }

                        // �����ֶ�����
                        if (!newFieldName.empty()) {
                            *it = newFieldName;
                        }

                        // �������ת������
                        string currentType = field["type"];
                        if (!newType.empty() && currentType != newType) {
                            if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos) && newType.find("string") != string::npos) {
                                // ����� int �� char ת��Ϊ string��ֱ������
                            }
                            else if (currentType.find("string") != string::npos && (newType.find("int") != string::npos || newType.find("char") != string::npos)) {
                                cerr << "�������ֶ����ʹ� string ת��Ϊ " << newType << "��" << endl;
                                output << "�������ֶ����ʹ� string ת��Ϊ " << newType << "��" << endl;
                                return;
                            }
                            else if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos || currentType.find("string") != string::npos) &&
                                newType.find(currentType.substr(0, currentType.find('['))) != string::npos) {
                                // ͬ���ʹ�С�仯����ֱ������
                            }
                            else {
                                cerr << "�������ֶ����ʹ� " << currentType << " ת��Ϊ " << newType << "��" << endl;
                                output << "�������ֶ����ʹ� " << currentType << " ת��Ϊ " << newType << "��" << endl;
                                return;
                            }
                        }

                        // ���µ���������
                        string updatedFields;
                        for (size_t i = 0; i < fields.size(); ++i) {
                            updatedFields += fields[i];
                            if (i != fields.size() - 1) {
                                updatedFields += ",";
                            }
                        }
                        fileLines[3] = updatedFields;

                        // д���޸ĺ������
                        ofstream outFile(datFilePath, ios::trunc | ios::binary);
                        for (const auto& updatedLine : fileLines) {
                            outFile << updatedLine << '\n';
                        }
                        outFile.close();
                    }

                    cout << "�ֶ� " << oldFieldName << " �ѳɹ��޸�Ϊ " << (newFieldName.empty() ? oldFieldName : newFieldName);
                    output << "�ֶ� " << oldFieldName << " �ѳɹ��޸�Ϊ " << (newFieldName.empty() ? oldFieldName : newFieldName);
                    if (!newType.empty()) {
                        cout << "�������޸�Ϊ " << newType;
                        output << "�������޸�Ϊ " << newType;
                    }
                    cout << endl;
                    output << endl;
                    datFile.close();
                    // ���û��ܺ���
                    tables = databaseTables[databaseName];
                    consolidateToDatabaseFile(databaseName, tables);
                    return;
                }
            }

            if (!fieldFound) { // ���δ�ҵ��ֶ�
                cerr << "δ�ҵ��ֶ�����: " << oldFieldName << endl;
                output << "δ�ҵ��ֶ�����: " << oldFieldName << endl;
                return;
            }


        }
    }

    cerr << "δ�ҵ���: " << tableName << endl;
    output << "δ�ҵ���: " << tableName << endl;

}

//��������////
void processSQLCommands(const string& tsql, map<string, vector<Table>>& databaseTables) {
    try {
        //output << "�յ���sql@" << tsql << "@" << endl;
        string sql = tsql;
        sql = trim(sql);
        //output << "���ڵ�sql@" << sql << "@" << endl;
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
            cout << "��⵽����save���������ݡ�\n";
            output << "��⵽����save���������ݡ�\n";
            saveTablesToDatabaseFiles(databaseTables); // ��������
        }
        else {
            cerr << "��֧�ֵĲ�����" << sql << endl;
            output << "��֧�ֵĲ�����" << sql << endl;
        }
    }
    catch (const exception& e) {
        cerr << "����: " << e.what() << endl;
        output << "����: " << e.what() << endl;
    }
}