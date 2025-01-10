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
    connect(ui.table_result, &QTableWidget::itemChanged, this, &MYDBSM::onEditTable);
    connect(ui.table_result->selectionModel(), &QItemSelectionModel::selectionChanged, this, &MYDBSM::onSelectTable);
    connect(ui.table_result, &QTableWidget::cellDoubleClicked, this, &MYDBSM::onDoubleClickTable);
}


MYDBSM::~MYDBSM()
{}

void MYDBSM::onButtonChoiceClicked()
{
    // �����ļ�ѡ��Ի���
    loaded = false;
    filePath = QFileDialog::getOpenFileName(this, "select database", "", "all files (*.*);;database file (*.dbf);;json file (*.json)");

    // ����ļ�·����Ϊ�գ�����ʾ��ѡ�ļ���·��
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "�ļ�ѡ��", "��ѡ����ļ���: " + filePath);
        ui.label_root->setText(filePath);
        refreshTableFromFile();
    }


    ifstream inFile(filePath.toStdString());
    if (!inFile.is_open()) {
        QMessageBox::information(this, "Alert", "�޷������ݿ��ļ�");
        return;
    }
    try {
        json dbJson;
        inFile >> dbJson;
        inFile.close();
        int rowNo = 0;
        ui.table_result->setRowCount(0);
        ui.table_result->setColumnCount(6);
        ui.table_result->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch); // ƽ��
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
                        if(i==0||i==3||i==4||i==5)
                            t->setFlags(t->flags() & ~Qt::ItemIsEditable);//��ֹ�༭table_name,key_flag,null_flag,valid_flag
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
            int row = ui.table_info->rowCount();  // ��ȡ��ǰ����
            ui.table_info->insertRow(row);  // �����һ�в�������
            ui.table_info->setItem(i, 0, new QTableWidgetItem(QString::fromStdString(res[i])));  // ���ָ���ַ���
        }*/
        for (int i = 0; i < res.size(); i++) {
            ui.list_info->addItem(QString::fromStdString(res[i]));
            //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
        }
        

    }
    catch (...) {
        cout << "���Ϸ����ļ���";
        QMessageBox::information(this, "Alert", "Invalid Database FIle! ");
    }
    

}

// ȥ���ַ���ĩβ�Ķ���ո�
std::string rtrim(const std::string& str) {
    size_t end = str.find_last_not_of(" \t\r\n");
    if (end == std::string::npos)
        return "";
    return str.substr(0, end + 1);
}

// ���ַ����ж���ո��滻Ϊһ���ո�
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

// �����ַ�����ȥ��ÿ�еĶ���ո�
std::string processString(const std::string& input) {
    std::istringstream stream(input);
    std::string line;
    std::string result;

    while (std::getline(stream, line)) {
        // ȥ��ÿ��ĩβ�Ķ���ո�
        line = rtrim(line);

        // ѹ���м����ո�Ϊһ���ո�
        line = compressSpaces(line);

        // ��Ӵ������е�����У�����ӻ��з�
        result += line + "\n";
    }

    // ɾ�����ĩβ�Ļ��з�
    while (!result.empty() && result[result.size() - 1] == '\n') {
        result = result.substr(0, result.size() - 1);
    }

    return result;
}




void MYDBSM::onCommitCommand() {
    string temp = ui.text_command->document()->toPlainText().toStdString();
    vector<string> res = streamParse(processString(temp));
    for (int i = 0; i < res.size(); i++) {
        ui.list_info->addItem(QString::fromStdString(res[i]));
        //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
    }
}


void MYDBSM::onEditTable(QTableWidgetItem* item) {
    //QMessageBox::information(this, "Help","Modifying");
    int row = item->row();
    int column = item->column();

    // ��ȡ�޸�ǰ������
    QString oldValue = item->data(Qt::EditRole).toString();

    // ��ȡ�޸ĺ������
    QString newValue = item->text();
    QString fieldname, tablename;
    tablename = ui.table_result->item(row, 0)->text();
    if(column>=1)
        fieldname = ui.table_result->item(row, 1)->text();
    QString databasename = pureFilePath;
    if (loaded) {
        //����
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
        
        for (int i = 0; i < res.size(); i++) {
            ui.list_info->addItem(QString::fromStdString(res[i]));
        }
    }

}

void MYDBSM::onSelectTable(const QItemSelection& selected, const QItemSelection& deselected) {
    if (!selected.isEmpty()) {
        // ��ȡ��ǰѡ�еĵ�Ԫ�񣨿��Դ�������Ԫ�񣬵��������ÿ��ֻѡ��һ����
        QModelIndex currentIndex = selected.indexes().first();
        selecting_field= ui.table_result->item(currentIndex.row(), 1)->text();
        //qDebug() << "Currently selected cell (" << currentIndex.row() << "," << currentIndex.column() << ") contains:" << currentText;
    }
}

void MYDBSM::onDoubleClickTable(int row, int column) {
    QMessageBox::information(this, "Help", QString::fromStdString("clicking "+to_string(row) + "," + to_string(column)));

}