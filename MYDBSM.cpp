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
    // 弹出文件选择对话框
    filePath = QFileDialog::getOpenFileName(this, "select database", "", "all files (*.*);;database file (*.dbf);;json file (*.json)");

    // 如果文件路径不为空，则显示所选文件的路径
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "文件选择", "您选择的文件是: " + filePath);
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
void MYDBSM::onCommitCommand() {
    vector<string> res = streamParse(ui.text_command->document()->toRawText().toStdString());
    for (int i = 0; i < res.size(); i++) {
        ui.list_info->addItem(QString::fromStdString(res[i]));
        //QMessageBox::information(this, "Help", QString::fromStdString(res[i]));
    }
}
