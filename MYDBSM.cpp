#include "MYDBSM.h"
#include "ui_MYDBSM.h"
#include <string>
#include <algorithm>
#include <regex>
#include "reader.h"
using namespace std;

mss mapdata;
QString filePath = "";
QString pureFilePath = "";
QString selecting_field;
bool loaded = false;
MYDBSM::MYDBSM(QWidget *parent)
    : QMainWindow(parent)
{
    ui.setupUi(this);
    //connect(ui->aa, &QPushButton::clicked, this, &MYDBSM::handleClick);

    connect(ui.button_choice, &QPushButton::clicked, this, &MYDBSM::onButtonChoiceClicked);
    connect(ui.button_commit, &QPushButton::clicked, this, &MYDBSM::onCommitCommand);
    connect(ui.button_refresh, &QPushButton::clicked, this, &MYDBSM::onButtonRefreshClicked);
    connect(ui.table_result, &QTableWidget::itemChanged, this, &MYDBSM::onEditTable);
    connect(ui.table_result->selectionModel(), &QItemSelectionModel::selectionChanged, this, &MYDBSM::onSelectTable);
    connect(ui.table_result, &QTableWidget::cellDoubleClicked, this, &MYDBSM::onDoubleClickTable);

}


MYDBSM::~MYDBSM()
{}

void MYDBSM::onButtonChoiceClicked()
{
    // 弹出文件选择对话框
    loaded = false;
    filePath = QFileDialog::getOpenFileName(this, "select database", "", "all files (*.*);;database file (*.dbf);;json file (*.json)");

    // 如果文件路径不为空，则显示所选文件的路径
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "文件选择", "您选择的文件是: " + filePath);
        ui.label_root->setText(filePath);
        refreshTableFromFile();
    }


    ifstream inFile(filePath.toStdString());
    if (!inFile.is_open()) {
        QMessageBox::information(this, "Alert", "cannot open database file!");
        return;
    }
    try {
        json dbJson;
        inFile >> dbJson;
        inFile.close();
        int rowNo = 0;
        ui.table_result->setRowCount(0);
        ui.table_result->setColumnCount(6);
        ui.table_result->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch); // 平分
        ui.table_result->horizontalHeader()->setVisible(true);
        ui.table_result->setHorizontalHeaderItem(0, new QTableWidgetItem("TableName"));
        ui.table_result->setHorizontalHeaderItem(1, new QTableWidgetItem("FieldName"));
        ui.table_result->setHorizontalHeaderItem(2, new QTableWidgetItem("Type"));
        ui.table_result->setHorizontalHeaderItem(3, new QTableWidgetItem("isKey"));
        ui.table_result->setHorizontalHeaderItem(4, new QTableWidgetItem("allowNull"));
        ui.table_result->setHorizontalHeaderItem(5, new QTableWidgetItem("isValid"));

        for (auto& tableJson : dbJson["tables"]) {
            int columnNo = 0;
            //ui.table_result->setRowCount(ui.table_result->rowCount() + 1);
            for (auto& field : tableJson["fields"]) {
                ui.table_result->setRowCount(ui.table_result->rowCount() + 1);
                rowNo++;
                //QMessageBox::information(this, "Help", QString::fromStdString(string(tableJson["table_name"])));
                vector<string> headers ({"table_name","name","type","key_flag","null_flag","valid_flag" });
                QTableWidgetItem* t;
                for (int i = 0; i < headers.size(); i++)
                {
                    if(i==0)
                        ui.table_result->setItem(rowNo - 1, 0, new QTableWidgetItem(QString::fromStdString(tableJson[headers[0]])));
                    else
                        ui.table_result->setItem(rowNo - 1, i, new QTableWidgetItem(QString::fromStdString(field[headers[i]])));
                    t = ui.table_result->item(rowNo - 1, i);
                    if (t) {
                        //if(i==0||i==3||i==4||i==5)
                        if (i == 0)
                            t->setFlags(t->flags() & ~Qt::ItemIsEditable);//禁止编辑table_name,key_flag,null_flag,valid_flag
                    }
                }
                
            }
        }
        loaded = true;
    }
    catch (...) {
        inFile.close();
        QMessageBox::information(this, "Alert", "Invalid Database File!");
        ui.table_result->horizontalHeader()->setVisible(false);
        ui.table_result->setRowCount(0);
        ui.table_result->setColumnCount(6);
        loaded = false;
    }
}
bool endsWith(const std::string& str, const std::string& suffix) {
    if (str.length() >= suffix.length()) {
        return str.substr(str.length() - suffix.length()) == suffix;
    }
    return false;
}
void MYDBSM::refreshTableFromFile() {
    try{
        //mapdata=readMapFromJsonFile(filePath.toStdString());
        string temps = filePath.toStdString();
        vector<string> res;
        if (endsWith(filePath.toStdString(), ".dbf")) {
            pureFilePath = QString::fromStdString(temps.substr(0, temps.size() - 4));
            res = streamParse("READ DATABASE " + pureFilePath.toStdString() + ";");
        }
        else {
            pureFilePath = QString::fromStdString(temps.substr(0, temps.size()));
            res = streamParse("READ DATABASE " + pureFilePath.toStdString() + ";");
        }
        //res = streamParse("READ DATABASE C:/my_codes/MYDBSM/DB;");
        /*for (int i = 0; i < res.size(); i++) {
            int row = ui.table_info->rowCount();  // 获取当前行数
            ui.table_info->insertRow(row);  // 在最后一行插入新行
            ui.table_info->setItem(i, 0, new QTableWidgetItem(QString::fromStdString(res[i])));  // 填充指定字符串
        }*/
        for (int i = 0; i < res.size(); i++) {
            ui.list_info->addItem(QString::fromStdString(res[i]));
            //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
        }
        

    }
    catch (...) {
        cout << "不合法的文件！";
        QMessageBox::information(this, "Alert", "Invalid Database FIle! ");
    }
    

}

// 去除字符串末尾的多余空格
std::string rtrim(const std::string& str) {
    size_t end = str.find_last_not_of(" \t\r\n");
    if (end == std::string::npos)
        return "";
    return str.substr(0, end + 1);
}

// 将字符串中多个空格替换为一个空格
std::string compressSpaces(const std::string& str) {
    std::string result;
    bool in_space = false;

    for (char ch : str) {
        if (ch == ' ') {
            if (!in_space) {
                result += ' ';
                in_space = true;
            }
        }
        else {
            result += ch;
            in_space = false;
        }
    }
    return result;
}

// 处理字符串，去除每行的多余空格
std::string processString(const std::string& input) {
    std::istringstream stream(input);
    std::string line;
    std::string result;

    while (std::getline(stream, line)) {
        // 去除每行末尾的多余空格
        line = rtrim(line);

        // 压缩中间多个空格为一个空格
        line = compressSpaces(line);

        // 添加处理后的行到结果中，并添加换行符
        result += line + "\n";
    }

    // 删除结果末尾的换行符
    while (!result.empty() && result[result.size() - 1] == '\n') {
        result = result.substr(0, result.size() - 1);
    }

    return result;
}

std::vector<std::string> split(const std::string& text, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(text);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void MYDBSM::loadTableFromString(string dataString) {
    string header, body;
    vector<string>headers, bodies;
    std::istringstream ds(dataString);
    getline(ds, header);
    headers = split(header, ',');
    ui.table_result2->setColumnCount(headers.size());
    ui.table_result2->horizontalHeader()->setVisible(true);
    ui.table_result2->setRowCount(0);
    for (int i = 0; i < ui.table_result2->columnCount(); i++)
    {
        try {
            if(i<headers.size())
            ui.table_result2->setHorizontalHeaderItem(i, new QTableWidgetItem(QString::fromStdString(headers[i]))); 
        }
        catch (...) {
            QMessageBox::information(this, "Help", "something invalid in table");
        }

    }
    while (std::getline(ds, body)) {
        bodies = split(body, ',');
        if (bodies.empty()) {
            continue;
        }
        ui.table_result2->setRowCount(ui.table_result2->rowCount() + 1);
        int row = ui.table_result2->rowCount() - 1;
        for (int i = 0; i < ui.table_result2->columnCount(); i++) {
            try { 
                
                if (i < bodies.size()) {
                    //QMessageBox::information(this, "Help", QString::fromStdString("@"+bodies[i] + "@"));
                    ui.table_result2->setItem(row, i, new QTableWidgetItem(QString::fromStdString(bodies[i]))); 
                }
                
            }
            catch (...) {
                QMessageBox::information(this, "Help", "something invalid in table");
            }
        }
    }
    return;
}


void MYDBSM::onCommitCommand() {
    string temp = ui.text_command->document()->toPlainText().toStdString();
    vector<string> res = streamParse(processString(temp));
    for (int i = 0; i < res.size(); i++) {
        ui.list_info->addItem(QString::fromStdString(res[i]));
        if (i >= 1 && res[i - 1] == "show Table") {
            loadTableFromString(res[i]);
            ui.label_current_table->setText(QString::fromStdString("Select Results"));
        }

        //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
    }
}


void MYDBSM::onEditTable(QTableWidgetItem* item) {
    //QMessageBox::information(this, "Help","Modifying");
    int row = item->row();
    int column = item->column();

    // 获取修改前的内容
    QString oldValue = item->data(Qt::EditRole).toString();

    // 获取修改后的内容
    QString newValue = item->text();
    QString fieldname, tablename,type,isKey,allowNull,isValid;
    tablename = ui.table_result->item(row, 0)->text();
    //QMessageBox::information(this, "Help", QString::fromStdString(""+to_string(ui.table_result->columnCount())));
    if (column >= 1) {
        fieldname = ui.table_result->item(row, 1)->text();
    }
    if (column >= 2) {
        type = ui.table_result->item(row, 2)->text();
    }
    if (column >= 3) {
        isKey = ui.table_result->item(row, 3)->text();
    }
    if (column >= 4) {
        allowNull = ui.table_result->item(row, 4)->text();
    }
    if (column >= 5) {
        isValid = ui.table_result->item(row, 5)->text();
    }
        

    QString databasename = pureFilePath;
    if (loaded) {
        fieldname = ui.table_result->item(row, 1)->text();
        type = ui.table_result->item(row, 2)->text();
        isKey = ui.table_result->item(row, 3)->text();
        allowNull = ui.table_result->item(row, 4)->text();
        isValid = ui.table_result->item(row, 5)->text();


        //保存
        vector<string> res;
        //QMessageBox::information(this, "Help", oldValue==newValue?"y" :"n");
        if(column==1)////MODIFY FIELD oldName TO newName IN tableName IN databaseName;
        {
            res = streamParse("MODIFY FIELD " + selecting_field.toStdString() + " TO " + newValue.toStdString() + " IN " + tablename.toStdString() + " IN " + pureFilePath.toStdString() + ";");
        }
        if (column == 2)// MODIFY FIELD oldName TYPE newType IN tableName IN databaseName;
        {
            res = streamParse("MODIFY FIELD " + fieldname.toStdString() + " TYPE " + newValue.toStdString() + " IN " + tablename.toStdString() + " IN " + pureFilePath.toStdString() + ";");
        }
        if (column == 3) {// EDIT TABLE tablename (fieldname,type, iskey,allowNULL,isValid)in db
            //QMessageBox::information(this, "Help", "Send command:EDIT TABLE " + tablename.toStdString() + " ( " + fieldname.toStdString() + " " + type.toStdString() + " " + isKey.toStdString() + " " + allowNull.toStdString() + " " + isValid.toStdString() + " ) IN " + pureFilePath.toStdString() + ";" );
            //string ts= "EDIT TABLE " + tablename.toStdString() + " ( " + fieldname.toStdString() + " " + type.toStdString() + " " + isKey.toStdString() + " " + allowNull.toStdString() + " " + isValid.toStdString() + " ) IN " + pureFilePath.toStdString() + ";";
            //QMessageBox::information(this, "Help", QString::fromStdString(ts));
            //QMessageBox::information(this, "Help", QString::fromStdString(ts));
            res = streamParse("EDIT TABLE " + tablename.toStdString()+ " ( " + fieldname.toStdString()+" " + type.toStdString() + " " + isKey.toStdString()+" "+ allowNull.toStdString() +" " + isValid.toStdString() + " ) IN " + pureFilePath.toStdString() + ";");
        }
        if (column == 4) {// EDIT TABLE tablename (fieldname,type, iskey,allowNULL,isValid)in db
            //string ts = "EDIT TABLE " + tablename.toStdString() + " ( " + fieldname.toStdString() + " " + type.toStdString() + " " + isKey.toStdString() + " " + allowNull.toStdString() + " " + isValid.toStdString() + " ) IN " + pureFilePath.toStdString() + ";";
            //QMessageBox::information(this, "Help", QString::fromStdString(ts));
            res = streamParse("EDIT TABLE " + tablename.toStdString() + " ( " + fieldname.toStdString() + " " + type.toStdString() + " " + isKey.toStdString() + " " + allowNull.toStdString() + " " + isValid.toStdString() + " ) IN " + pureFilePath.toStdString() + ";");
        }
        if (column == 5) {// EDIT TABLE tablename (fieldname,type, iskey,allowNULL,isValid)in db
            res = streamParse("EDIT TABLE " + tablename.toStdString() + " ( " + fieldname.toStdString() + " " + type.toStdString() + " " + isKey.toStdString() + " " + allowNull.toStdString() + " " + isValid.toStdString() + " ) IN " + pureFilePath.toStdString() + ";");
        }
        for (int i = 0; i < res.size(); i++) {
            ui.list_info->addItem(QString::fromStdString(res[i]));
        }
    }

}

void MYDBSM::onSelectTable(const QItemSelection& selected, const QItemSelection& deselected) {
    if (!selected.isEmpty()) {
        // 获取当前选中的单元格（可以处理多个单元格，但这里假设每次只选中一个）
        QModelIndex currentIndex = selected.indexes().first();
        selecting_field= ui.table_result->item(currentIndex.row(), 1)->text();
        //qDebug() << "Currently selected cell (" << currentIndex.row() << "," << currentIndex.column() << ") contains:" << currentText;
    }
}

void MYDBSM::onDoubleClickTable(int row, int column) {
    //QMessageBox::information(this, "Help", QString::fromStdString("clicking "+to_string(row) + "," + to_string(column)));
    string temps = filePath.toStdString();
    string tablename = ui.table_result->item(row, 0)->text().toStdString();
    vector<string> res ;
    res = streamParse("SHOW CONTENT " + tablename + " in "+ pureFilePath.toStdString() + ";");
    for (int i = 0; i < res.size(); i++) {
        ui.list_info->addItem(QString::fromStdString(res[i]));
        //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
    }
    //string dataString = res[res.size() - 1];
    loadTableFromString(res[res.size() - 1]);
    /*
    //QMessageBox::information(this, "Help", QString::fromStdString(dataString));
    string header,body;
    vector<string>headers, bodies;
    std::istringstream ds(dataString);
    getline(ds,header);
    headers = split(header, ',');
    ui.table_result2->setColumnCount(headers.size());
    ui.table_result2->horizontalHeader()->setVisible(true);
    ui.table_result2->setRowCount(0);
    for (int i = 0; i < ui.table_result2->columnCount(); i++)
    {
        try {ui.table_result2->setHorizontalHeaderItem(i,new QTableWidgetItem(QString::fromStdString(headers[i])));}
        catch (...) {
            QMessageBox::information(this, "Help", "something invalid in table");
        }
        
    }
    while (std::getline(ds, body)) {
        ui.table_result2->setRowCount(ui.table_result2->rowCount()+1);
        int row = ui.table_result2->rowCount()-1;
        bodies = split(body,',');
        for (int i = 0; i < ui.table_result2->columnCount(); i++) {
            try{ui.table_result2->setItem(row, i, new QTableWidgetItem(QString::fromStdString(bodies[i])));}
            catch (...) {
                QMessageBox::information(this, "Help", "something invalid in table");
            }
        }
    }*/
    ui.label_current_table->setText(QString::fromStdString(tablename));

}

void MYDBSM::onButtonRefreshClicked() {
    //QMessageBox::information(this, "Help", QString::fromStdString("refresh"));
    loaded = false;
    // 如果文件路径不为空，则显示所选文件的路径
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "文件选择", "您选择的文件是: " + filePath);
        ui.label_root->setText(filePath);
        refreshTableFromFile();
    }


    ifstream inFile(filePath.toStdString());
    if (!inFile.is_open()) {
        QMessageBox::information(this, "Alert", "cannot open database file!");
        return;
    }
    try {
        json dbJson;
        inFile >> dbJson;
        inFile.close();
        int rowNo = 0;
        ui.table_result->setRowCount(0);
        ui.table_result->setColumnCount(6);
        ui.table_result->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch); // 平分
        ui.table_result->horizontalHeader()->setVisible(true);
        ui.table_result->setHorizontalHeaderItem(0, new QTableWidgetItem("TableName"));
        ui.table_result->setHorizontalHeaderItem(1, new QTableWidgetItem("FieldName"));
        ui.table_result->setHorizontalHeaderItem(2, new QTableWidgetItem("Type"));
        ui.table_result->setHorizontalHeaderItem(3, new QTableWidgetItem("isKey"));
        ui.table_result->setHorizontalHeaderItem(4, new QTableWidgetItem("allowNull"));
        ui.table_result->setHorizontalHeaderItem(5, new QTableWidgetItem("isValid"));

        for (auto& tableJson : dbJson["tables"]) {
            int columnNo = 0;
            //ui.table_result->setRowCount(ui.table_result->rowCount() + 1);
            for (auto& field : tableJson["fields"]) {
                ui.table_result->setRowCount(ui.table_result->rowCount() + 1);
                rowNo++;
                //QMessageBox::information(this, "Help", QString::fromStdString(string(tableJson["table_name"])));
                vector<string> headers({ "table_name","name","type","key_flag","null_flag","valid_flag" });
                QTableWidgetItem* t;
                for (int i = 0; i < headers.size(); i++)
                {
                    if (i == 0)
                        ui.table_result->setItem(rowNo - 1, 0, new QTableWidgetItem(QString::fromStdString(tableJson[headers[0]])));
                    else
                        ui.table_result->setItem(rowNo - 1, i, new QTableWidgetItem(QString::fromStdString(field[headers[i]])));
                    t = ui.table_result->item(rowNo - 1, i);
                    if (t) {
                        //if(i==0||i==3||i==4||i==5)
                        if (i == 0)
                            t->setFlags(t->flags() & ~Qt::ItemIsEditable);//禁止编辑table_name,key_flag,null_flag,valid_flag
                    }
                }

            }
        }
        loaded = true;
    }
    catch (...) {
        inFile.close();
        QMessageBox::information(this, "Alert", "Invalid Database File!");
        ui.table_result->horizontalHeader()->setVisible(false);
        ui.table_result->setRowCount(0);
        ui.table_result->setColumnCount(6);
        loaded = false;
    }
    return;
}