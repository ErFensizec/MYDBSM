#include "MYDBSM.h"
#include "ui_MYDBSM.h"
#include <string>
#include "reader.h"
using namespace std;

mss mapdata;
QString filePath = "";
MYDBSM::MYDBSM(QWidget *parent)
    : QMainWindow(parent)
{
    ui.setupUi(this);
    //connect(ui->aa, &QPushButton::clicked, this, &MYDBSM::handleClick);

    connect(ui.button_choice, &QPushButton::clicked, this, &MYDBSM::onButtonChoiceClicked);
    connect(ui.button_commit, &QPushButton::clicked, this, &MYDBSM::onCommitCommand);
}


MYDBSM::~MYDBSM()
{}

void MYDBSM::onButtonChoiceClicked()
{
    // �����ļ�ѡ��Ի���
    filePath = QFileDialog::getOpenFileName(this, "select database", "", "all files (*.*);;database file (*.dbf);;json file (*.json)");

    // ����ļ�·����Ϊ�գ�����ʾ��ѡ�ļ���·��
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "�ļ�ѡ��", "��ѡ����ļ���: " + filePath);
        ui.label_root->setText(filePath);
        refreshTableFromFile();
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
            res = streamParse("READ DATABASE " + temps.substr(0, temps.size() - 4) + ";");
        }
        else {
            res = streamParse("READ DATABASE " + temps + ";");
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
void MYDBSM::onCommitCommand() {
    vector<string> res = streamParse(ui.text_command->document()->toRawText().toStdString());
    for (int i = 0; i < res.size(); i++) {
        ui.list_info->addItem(QString::fromStdString(res[i]));
        //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
    }
}
