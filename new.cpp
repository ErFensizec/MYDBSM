#include <filesystem>

namespace fs = std::filesystem;

// �������Ϣ����Ӧ���ݿ�� JSON �ļ�//
void saveTablesToDatabaseFiles(const map<string, vector<Table>>& databaseTables) {
    for (const auto& [databaseName, tables] : databaseTables) {
        string filePath = databaseName + ".dbf";
        json dbJson;

        // ����ļ����ڣ���ȡ��������
        ifstream inFile(filePath);
        if (inFile.is_open()) {
            try {
                inFile >> dbJson; // ���������ļ�
            }
            catch (const exception& e) {
                cerr << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
                output << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
                inFile.close();
                continue;
            }
            inFile.close();
        }

        // ����ļ���û�� "tables" �ֶΣ����ʼ��Ϊ������
        if (!dbJson.contains("tables")) {
            dbJson["tables"] = json::array();
        }

        // ��ȡ���б�������
        set<string> existingTableNames;
        for (const auto& tableJson : dbJson["tables"]) {
            if (tableJson.contains("table_name")) {
                existingTableNames.insert(tableJson["table_name"]);
            }
        }

        // ��������±�����
        for (const auto& table : tables) {
            // ����±����ֶε� keyFlag ������
            int keyCount = 0;
            for (const auto& field : table.fields) {
                if (field.keyFlag == "KEY") {
                    keyCount++;
                }

                // ����ֶ������Ƿ�Ϸ�
                const string& type = field.type;
                if (!regex_match(type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "�� " << table.tableName << " ���ֶ����� " << type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                    output << "�� " << table.tableName << " ���ֶ����� " << type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                    return;
                }
            }

            if (keyCount > 1) {
                cerr << "�� " << table.tableName << " �д��ڶ���ֶα��Ϊ KEY��������" << endl;
                output << "�� " << table.tableName << " �д��ڶ���ֶα��Ϊ KEY��������" << endl;
                return;
            }

            if (existingTableNames.count(table.tableName)) {
                // ������Ѵ��ڣ������ֶ�����
                for (auto& tableJson : dbJson["tables"]) {
                    if (tableJson["table_name"] == table.tableName) {
                        tableJson["field_count"] = table.fields.size(); // �����ֶ�����
                    }
                }
            }
            else {
                // ��������ڣ�����±���Ϣ
                json newTable = table.toJson();
                newTable["field_count"] = table.fields.size(); // �����ֶ�����
                dbJson["tables"].push_back(newTable);
            }

        }

        // д���ļ�
        ofstream outFile(filePath);
        if (outFile.is_open()) {
            outFile << dbJson.dump(4); // ʹ�� 4 ���ո�����
            outFile.close();
            cout << "����Ϣ�ѱ��浽 " << filePath << endl;
            output << "����Ϣ�ѱ��浽 " << filePath << endl;

        }
        else {
            cerr << "�޷����ļ� " << filePath << " ����д�롣" << endl;
            output << "�޷����ļ� " << filePath << " ����д�롣" << endl;
        }
    }
}

// �༭���ֶ�//
void editTableFields(const string& tableName, const string& databaseName, const vector<Field>& newFields) {
    string filePath = databaseName + ".dbf";
    string datFilePath = databaseName + "_" + tableName + ".dat";
    ifstream inFile(filePath);
    json dbJson;

    // ��ȡ��������
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // �������� JSON �ļ�
        }
        catch (const exception& e) {
            cerr << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
            output << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "�� " << tableName << " �����ڡ�" << endl;
        output << "�� " << tableName << " �����ڡ�" << endl;
        return;
    }

    // ��� .dat �ļ��Ƿ������ݼ�¼
    ifstream datFile(datFilePath);
    if (datFile.is_open()) {
        string line;
        getline(datFile, line); // ��������
        getline(datFile, line); // ������¼��
        getline(datFile, line); // �����ֶ�����
        getline(datFile, line); // �����ֶ���
        if (getline(datFile, line)) { // ����Ƿ������ݼ�¼
            cerr << "�� " << tableName << " �����ݼ�¼���޷��޸��ֶ����͡�" << endl;
            output << "�� " << tableName << " �����ݼ�¼���޷��޸��ֶ����͡�" << endl;
            datFile.close();
            return;
        }
        datFile.close();
    }

    // ���ұ��༭�ֶ�
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == tableName) {
            found = true;

            // �����ֶΣ�����ԭ�ֶΣ�����ƥ���ֶΣ�������ֶ�
            auto& existingFields = tableJson["fields"];
            for (const auto& newField : newFields) {
                bool fieldExists = false;
                // ����ֶ������Ƿ�Ϸ�
                if (!regex_match(newField.type, regex("(int|char|string)\\[\\d+\\]"))) {
                    cerr << "�����ֶ����� " << newField.type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                    output << "�����ֶ����� " << newField.type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                    return;
                }
                for (auto& existingField : existingFields) {
                    if (existingField["name"] == newField.name) {
                        // ���������ֶε�����
                        existingField["type"] = newField.type;
                        existingField["key_flag"] = newField.keyFlag;
                        existingField["null_flag"] = newField.nullFlag;
                        existingField["valid_flag"] = newField.validFlag;
                        fieldExists = true;
                        break;
                    }
                }

                // ����ֶβ����ڣ����Ϊ���ֶ�
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

            // ������ֶ�����������ȷ�������ֶε�������־������Ϊ������
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
        cerr << "δ�ҵ��� " << tableName << "��" << endl;
        output << "δ�ҵ��� " << tableName << "��" << endl;
        return;
    }

    // д���ļ�
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // ʹ�� 4 ���ո�����
        outFile.close();
        cout << "�� " << tableName << " ���ֶ��Ѹ��¡�\n";
        output << "�� " << tableName << " ���ֶ��Ѹ��¡�\n";
    }
    else {
        cerr << "�޷����ļ� " << filePath << " ����д�롣" << endl;
        output << "�޷����ļ� " << filePath << " ����д�롣" << endl;
    }
}

// ɾ����//
void dropTable(const string& tableName, const string& databaseName) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    json dbJson;

    // ��ȡ��������
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // �������� JSON �ļ�
        }
        catch (const exception& e) {
            cerr << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
            output << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "�� " << tableName << " �����ڡ�" << endl;
        output << "�� " << tableName << " �����ڡ�" << endl;
        return;
    }
    // ��鲢ɾ����Ӧ��¼
    string datFile = databaseName + "_" + tableName + ".dat";

    if (fs::exists(datFile)) {
        output << "��" << tableName << "�����м�¼һ��ɾȥ��\n";
        fs::remove(datFile);
    }
    // ɾ����
    auto& tables = dbJson["tables"];
    auto it = find_if(tables.begin(), tables.end(), [&](const json& tableJson) {
        return tableJson["table_name"] == tableName;
        });

    if (it != tables.end()) {
        tables.erase(it);
        cout << "�� " << tableName << " ��ɾ����\n";
        output << "�� " << tableName << " ��ɾ����\n";
    }
    else {
        cerr << "δ�ҵ��� " << tableName << "��\n";
        output << "δ�ҵ��� " << tableName << "��\n";
        return;
    }


    // д���ļ�
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // ʹ�� 4 ���ո�����
        outFile.close();
    }
    else {
        cerr << "�޷����ļ� " << filePath << " ����д�롣" << endl;
        output << "�޷����ļ� " << filePath << " ����д�롣" << endl;
    }
}

// �� JSON �ļ��������ݿ����Ϣ
map<string, vector<Table>> loadDatabaseTables(const string& jsonFilePath) {
    ifstream inFile(jsonFilePath);
    if (!inFile.is_open()) {
        cerr << "�޷����ļ�: " << jsonFilePath << endl;
        output << "�޷����ļ�: " << jsonFilePath << endl;
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

    // ��֤ÿ���ֶε����ͺͳ���
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

void deleteRecordFromTableFile(const string& databaseName, const string& tableName, const vector<pair<string, string>>& conditions, map<string, vector<Table>>& databaseTables) {
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
            output << "�ֶ� " << condition.first << " �����ڡ�" << endl;
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
            output << "�޷����ļ�����д�ز�����" << endl;
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
        output << "��¼ɾ���ɹ������Ϊ��Ч����" << endl;
    }
    else {
        cerr << "δ�ҵ����������ļ�¼�����¼����Ч��" << endl;
        output << "δ�ҵ����������ļ�¼�����¼����Ч��" << endl;
    }
}

int validateFieldValue(const Field& field, const string& value) {
    // ��ȡ�ֶ����ͺͳ���
    regex typeRegex(R"((int|char|string)\[(\d+)\])", regex_constants::icase);
    smatch match;
    if (regex_match(field.type, match, typeRegex)) {
        string type = match[1];  // ��ȡ�ֶ�����
        int length = stoi(match[2]);  // ��ȡ�ֶγ���

        // ��֤�ֶγ���
        if (value.length() > length) {
            cerr << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ���ȳ������� (" << length << ")��" << endl;
            output << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ���ȳ������� (" << length << ")��" << endl;
            return false;
        }

        // ��֤�ֶ�����
        if (type == "int") {
            if (!regex_match(value, regex(R"(\d+)"))) {
                cerr << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ int ����Ҫ��" << endl;
                output << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ int ����Ҫ��" << endl;
                return false;
            }
        }
        else if (type == "char") {
            if (!regex_match(value, regex(R"([a-zA-Z]+)"))) {
                cerr << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ char ����Ҫ��" << endl;
                output << "�ֶ� " << field.name << " ��ֵ \"" << value << "\" ������ char ����Ҫ��" << endl;
                return false;
            }
        }
        // string �������������֤
    }
    else {
        cerr << "�ֶ����Ͷ��� \"" << field.type << "\" �޷�������" << endl;
        output << "�ֶ����Ͷ��� \"" << field.type << "\" �޷�������" << endl;
        return false;
    }
    return true;
}

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

    // ���Ŀ���ֶ��Ƿ����
    if (fieldIndexMap.find(targetField) == fieldIndexMap.end()) {
        cerr << "�ֶ� " << targetField << " �����ڡ�" << endl;
        output << "�ֶ� " << targetField << " �����ڡ�" << endl;
        datFile.close();
        return;
    }

    // ��������ֶ��Ƿ����
    for (const auto& condition : conditions) {
        if (fieldIndexMap.find(condition.first) == fieldIndexMap.end()) {
            cerr << "�ֶ� " << condition.first << " �����ڡ�" << endl;
            output << "�ֶ� " << condition.first << " �����ڡ�" << endl;
            datFile.close();
            return;
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

    // �����ֶ��������͵�ӳ��
    map<string, string> fieldTypes;
    for (const auto& field : table.fields) {
        fieldTypes[field.name] = field.type;
    }
    // ���Ŀ���ֶ��Ƿ���ڲ���ȡ����
    if (fieldTypes.find(targetField) == fieldTypes.end()) {
        cerr << "�ֶ� " << targetField << " �������ڱ� " << tableName << " �С�" << endl;
        return;
    }
    string fieldType = fieldTypes[targetField];

    // ��֤Ŀ���ֶ�ֵ
    Field targetFieldInfo{ targetField, fieldType };
    if (validateFieldValue(targetFieldInfo, newValue) == false) {
        return;
    };

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

//��������//
void renameTable(const string& oldTableName, const string& newTableName, const string& databaseName, map<string, vector<Table>>& databaseTables) {
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    json dbJson;

    // ��ȡ��������
    if (inFile.is_open()) {
        try {
            inFile >> dbJson; // �������� JSON �ļ�
        }
        catch (const exception& e) {
            cerr << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
            output << "��ȡ�ļ� " << filePath << " ʱ��������" << e.what() << endl;
            inFile.close();
            return;
        }
        inFile.close();
    }

    if (!dbJson.contains("tables")) {
        cerr << "�� " << oldTableName << " �����ڡ�" << endl;
        output << "�� " << oldTableName << " �����ڡ�" << endl;
        return;
    }

    // ���ұ�������
    bool found = false;
    for (auto& tableJson : dbJson["tables"]) {
        if (tableJson["table_name"] == oldTableName) {
            tableJson["table_name"] = newTableName;
            found = true;
            break;
        }
    }

    if (!found) {
        cerr << "δ�ҵ��� " << oldTableName << "��" << endl;
        output << "δ�ҵ��� " << oldTableName << "��" << endl;
        return;
    }

    // ��鲢��������Ӧ�� .dat �ļ�
    string oldDatFile = databaseName + "_" + oldTableName + ".dat";
    string newDatFile = databaseName + "_" + newTableName + ".dat";

    if (fs::exists(oldDatFile)) {
        try {
            fs::rename(oldDatFile, newDatFile);
            cout << "���¼�ļ��Ѵ� " << oldDatFile << " ������Ϊ " << newDatFile << "��\n";
            output << "���¼�ļ��Ѵ� " << oldDatFile << " ������Ϊ " << newDatFile << "��\n";
        }
        catch (const fs::filesystem_error& e) {
            cerr << "���������¼�ļ�ʱ��������" << e.what() << endl;
            output << "���������¼�ļ�ʱ��������" << e.what() << endl;
            return;
        }
    }
    else {
        cout << "δ�ҵ����¼�ļ� " << oldDatFile << "�����ܱ��޶�Ӧ�����ļ���\n";
        output << "δ�ҵ����¼�ļ� " << oldDatFile << "�����ܱ��޶�Ӧ�����ļ���\n";
    }

    // д���ļ�
    ofstream outFile(filePath);
    if (outFile.is_open()) {
        outFile << dbJson.dump(4); // ʹ�� 4 ���ո�����
        outFile.close();
        cout << "�� " << oldTableName << " ��������Ϊ " << newTableName << "��\n";
        output << "�� " << oldTableName << " ��������Ϊ " << newTableName << "��\n";
    }
    else {
        cerr << "�޷����ļ� " << filePath << " ����д�롣" << endl;
        output << "�޷����ļ� " << filePath << " ����д�롣" << endl;
        return;
    }

    // ����Ƿ���Ҫ�������ݿ�
    if (databaseTables.find(databaseName) == databaseTables.end()) {
        cout << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        output << "��⵽���ݿ���δ���أ����Լ������ݿ⣺" << databaseName << endl;
        databaseTables = loadDatabaseTables(databaseName + ".dbf"); // ���� JSON �ļ�·��Ϊ "MyDB.dbf"
        if (databaseTables.find(databaseName) == databaseTables.end()) {
            cerr << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            output << "�������ݿ�ʧ�ܻ����ݿⲻ���ڣ�" << databaseName << endl;
            return;
        }
    }
    // �����ڴ��б���
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
        // ����д���һ��
        datFile.seekp(0, ios::beg);
        datFile << "~" << newTableName << '\n';
        datFile.close();
        cout << "�±��� " << newTableName << " �Ѹ���д����¼�ļ� " << newDatFile << " �ĵ�һ�С�\n";
        output << "�±��� " << newTableName << " �Ѹ���д����¼�ļ� " << newDatFile << " �ĵ�һ�С�\n";
    }
    else {
        cerr << "�޷��򿪱��¼�ļ� " << newDatFile << " ����д�롣" << endl;
        output << "�޷��򿪱��¼�ļ� " << newDatFile << " ����д�롣" << endl;
    }

    // ���û��ܺ���
    auto& tables = databaseTables[databaseName];
    consolidateToDatabaseFile(databaseName, tables);
}

//����������װ//
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
        cerr << "RENAME TABLE �﷨����" << sql << endl;
        output << "RENAME TABLE �﷨����" << sql << endl;
    }
}

//�ֶ�����//
void addFieldToTable(const string& tableName, const string& databaseName, const Field& newField, map<string, vector<Table>>& databaseTables) {
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

    // ����ֶ��Ƿ��Ѵ���
    for (const auto& field : table.fields) {
        if (field.name == newField.name) {
            cerr << "�ֶ� " << newField.name << " �Ѵ����ڱ� " << tableName << " �С�" << endl;
            output << "�ֶ� " << newField.name << " �Ѵ����ڱ� " << tableName << " �С�" << endl;
            return;
        }
    }
    // ����ֶ������Ƿ�Ϸ�
    if (!regex_match(newField.type, regex("(int|char|string)\\[\\d+\\]"))) {
        cerr << "�� " << table.tableName << " ���ֶ����� " << newField.type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
        output << "�� " << table.tableName << " ���ֶ����� " << newField.type << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
        return;
    }
    // ����Ƿ����������ֶΣ������ֶ�������
    if (newField.keyFlag == "KEY") {
        for (const auto& field : table.fields) {
            if (field.keyFlag == "KEY") {
                cerr << "�������������ֶΣ��޷�����������ֶ� " << newField.name << endl;
                output << "�������������ֶΣ��޷�����������ֶ� " << newField.name << endl;
                return;
            }
        }
    }
    int op = 0;
    string datFilePath = databaseName + "_" + table.tableName + ".dat";
    fstream datFile(datFilePath, ios::in);
    // ����Ƿ����зǿ��ֶΣ������ֶ��Ƿǿ�
    if (newField.nullFlag == "NO_NULL") {
        if (datFile.is_open()) {
            cerr << "���ļ� " << datFilePath << " ����,������ǿ��ֶ����" << endl;
            output << "���ļ� " << datFilePath << " ����,������ǿ��ֶ����" << endl;
            return;
        }
        else {
            op = -1;
        }

        // ��ȡ�ļ�������ȷ�����������Ƿ�����ǿ�Ҫ��
        vector<string> fileLines;
        string line;
        while (getline(datFile, line)) {
            fileLines.push_back(line);
        }

        // ���ӵ����п�ʼ��������
        if (fileLines.size() > 4) {
            for (size_t i = 4; i < fileLines.size(); ++i) {
                stringstream ss(fileLines[i]);
                string cell;
                int fieldCountInLine = 0;

                // ������е��ֶ���
                while (getline(ss, cell, ',')) {
                    fieldCountInLine++;
                }
                // ������ֶ�Ϊ�ǿգ��򱨴�
                if (newField.nullFlag == "NO_NULL") {
                    cerr << "�������ݣ������ֶ�Ϊ�ǿ��ֶΣ��޷�����ֶ� " << newField.name << endl;
                    output << "�������ݣ������ֶ�Ϊ�ǿ��ֶΣ��޷�����ֶ� " << newField.name << endl;
                    datFile.close();
                    return;
                }
            }
        }
        datFile.close();
    }

    // ������ֶε��ڴ�
    table.fields.push_back(newField);
    table.fieldCount = table.fields.size();

    // ���µ����ݿ��ļ�
    string filePath = databaseName + ".dbf";
    ifstream inFile(filePath);
    if (!inFile.is_open()) {
        cerr << "�޷������ݿ��ļ�: " << filePath << endl;
        output << "�޷������ݿ��ļ�: " << filePath << endl;
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

            tableJson["field_count"] = table.fieldCount; // �����ֶ�����
            break;
        }
    }

    // д�ظ��º�� JSON �ļ�
    ofstream outFile1(filePath);
    if (outFile1.is_open()) {
        outFile1 << dbJson.dump(4);
        outFile1.close();
        cout << "�ֶ� " << newField.name << " ��ӳɹ�����ǰ�ֶ�����: " << table.fieldCount << endl;
        output << "�ֶ� " << newField.name << " ��ӳɹ�����ǰ�ֶ�����: " << table.fieldCount << endl;
    }
    else {
        cerr << "�޷��������ݿ��ļ�: " << filePath << endl;
        output << "�޷��������ݿ��ļ�: " << filePath << endl;
    }

    fstream datFile1(datFilePath, ios::in | ios::out);
    if (!datFile1.is_open()) {
        cerr << "�޷��򿪱��ļ�: " << datFilePath << endl;
        output << "�޷��򿪱��ļ�: " << datFilePath << endl;
        return;
    }

    // ��ȡ�ļ����������ݵ��ڴ�
    vector<string> fileLines;
    string line;
    while (getline(datFile1, line)) {
        fileLines.push_back(line);
    }

    // ȷ���ļ�������������
    if (fileLines.size() < 4) {
        cerr << "�ļ��������� 4 �У��޷��޸ĵ����͵����С�" << endl;
        output << "�ļ��������� 4 �У��޷��޸ĵ����͵����С�" << endl;
        datFile1.close();
        return;
    }

    // �޸ĵ����У������ֶ���
    int fieldCount = table.fields.size(); // ��ǰ�ֶ�����
    fileLines[2] = to_string(fieldCount); // ������Ϊ�ֶ���

    // �޸ĵ����У������ֶ�����
    string newLine = "Valid,";
    for (const auto& field : table.fields) {
        newLine += field.name + ",";
    }
    fileLines[3] = newLine; // ������Ϊ�ֶ�����

    // ��ȫ�����������е��ֶΣ�������ݲ��㣬��NULL��
    for (size_t i = 4; i < fileLines.size(); ++i) {
        stringstream ss(fileLines[i]);
        string cell;
        vector<string> row;
        while (getline(ss, cell, ',')) {
            row.push_back(cell);
        }

        // �����������С���µ��ֶ������򲹳�NULL
        while (row.size() < table.fields.size() + 1) {
            row.push_back("NULL");
        }

        // ���¹�����һ��
        string updatedLine = "";
        for (size_t j = 0; j < row.size(); ++j) {
            updatedLine += row[j] + ",";
        }
        //updatedLine.pop_back();  // ȥ�����һ������Ķ���
        fileLines[i] = updatedLine;
    }

    // ���޸ĺ������д���ļ�
    datFile.close(); // �ر��ļ���׼������д��
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

    cout << "�����к͵������޸���ɣ��ֶ�����" << fieldCount << ", �ֶ������Ѹ��¡�" << endl;
    output << "�����к͵������޸���ɣ��ֶ�����" << fieldCount << ", �ֶ������Ѹ��¡�" << endl;

    // ���û��ܺ���
    consolidateToDatabaseFile(databaseName, tables);
}

//�����ֶη�װ//
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
        cerr << "ADD FIELD �﷨����" << sql << endl;
        output << "ADD FIELD �﷨����" << sql << endl;
    }
}

// �ֶ�ɾ��//
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
    if (!datFile.is_open()) {
        cerr << "�޷��򿪱��ļ�: " << datFilePath << endl;
        output << "�޷��򿪱��ļ�: " << datFilePath << endl;
        return;
    }

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

//ɾ���ֶη�װ//
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
        cerr << "REMOVE FIELD �﷨����" << sql << endl;
        output << "REMOVE FIELD �﷨����" << sql << endl;
    }
}

//�޸��ֶ�//
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
            for (auto& field : tableJson["fields"]) {
                if (field["name"] == oldFieldName) {
                    // ������ֶ������Ƿ���Ч
                    if (!newType.empty() && !regex_match(newType, regex("(int|char|string)\\[\\d+\\]"))) {
                        cerr << "�� " << tableName << " ���ֶ����� " << newType << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
                        output << "�� " << tableName << " ���ֶ����� " << newType << " ��Ч����֧�� int[x], char[x], string[x] ��ʽ��" << endl;
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

                        // ֱ�Ӷ�ȡ���������һ���ֶ�
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
                            // ����� int �� char ת��Ϊ string
                            if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos) && newType.find("string") != string::npos) {
                                // ���� int �� char ת��Ϊ string
                                // �����κδ���ֱ������
                            }
                            // ������� string ת��Ϊ int �� char
                            else if (currentType.find("string") != string::npos && (newType.find("int") != string::npos || newType.find("char") != string::npos)) {
                                cerr << "�������ֶ����ʹ� string ת��Ϊ " << newType << "��" << endl;
                                output << "�������ֶ����ʹ� string ת��Ϊ " << newType << "��" << endl;
                                return;
                            }
                            // ����ͬ����֮��Ĵ�С�仯��int -> int��char -> char
                            else if ((currentType.find("int") != string::npos || currentType.find("char") != string::npos || currentType.find("string") != string::npos) &&
                                newType.find(currentType.substr(0, currentType.find('['))) != string::npos) {
                                // ���� int[x] תΪ int[y] �� char[x] תΪ char[y]
                                // �����κδ���ֱ������
                            }
                            else {
                                cerr << "�������ֶ����ʹ� " << currentType << " ת��Ϊ " << newType << "��" << endl;
                                output << "�������ֶ����ʹ� " << currentType << " ת��Ϊ " << newType << "��" << endl;
                                return;
                            }
                        }

                        // ���µ��������ݣ��������Ķ���
                        string updatedFields;
                        for (size_t i = 0; i < fields.size(); ++i) {
                            updatedFields += fields[i];
                            updatedFields += ",";
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
                }
            }

            return;
        }
    }

    cerr << "δ�ҵ���: " << tableName << endl;
    output << "δ�ҵ���: " << tableName << endl;
    // ���û��ܺ���
    consolidateToDatabaseFile(databaseName, tables);
}

//�޸��ֶ��������ͷ�װ//
void handleModifyField(const string& sql, map<string, vector<Table>>& databaseTables) {
    // ������ʽ֧�����������
    // 1. �޸��ֶ���������: MODIFY FIELD oldName TO newName TYPE newType IN tableName IN databaseName;
    // 2. ���޸��ֶ���: MODIFY FIELD oldName TO newName IN tableName IN databaseName;
    // 3. ���޸��ֶ�����: MODIFY FIELD oldName TYPE newType IN tableName IN databaseName;
    regex modifyRegexFull(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    regex modifyRegexNameOnly(R"(MODIFY FIELD (\w+)\s+TO\s+(\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);
    regex modifyRegexTypeOnly(R"(MODIFY FIELD (\w+)\s+TYPE\s+(\w+\[\d+\]|\w+)\s+IN\s+(\w+)\s+IN\s+(\S+);)", regex_constants::icase);

    smatch match;

    if (regex_match(sql, match, modifyRegexFull)) {
        // �޸��ֶ���������
        string oldFieldName = match[1];
        string newFieldName = match[2];
        string newType = match[3];
        string tableName = match[4];
        string databaseName = match[5];
        modifyFieldInTable(tableName, databaseName, oldFieldName, newFieldName, newType, databaseTables);
    }
    else if (regex_match(sql, match, modifyRegexNameOnly)) {
        // ���޸��ֶ���
        string oldFieldName = match[1];
        string newFieldName = match[2];
        string tableName = match[3];
        string databaseName = match[4];
        modifyFieldInTable(tableName, databaseName, oldFieldName, newFieldName, "", databaseTables);
    }
    else if (regex_match(sql, match, modifyRegexTypeOnly)) {
        // ���޸��ֶ�����
        string oldFieldName = match[1];
        string newType = match[2];
        string tableName = match[3];
        string databaseName = match[4];
        modifyFieldInTable(tableName, databaseName, oldFieldName, "", newType, databaseTables);
    }
    else {
        cerr << "MODIFY FIELD �﷨����" << sql << endl;
        output << "MODIFY FIELD �﷨����" << sql << endl;
    }
}

//��������//
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