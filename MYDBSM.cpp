#include "MYDBSM.h"
#include "ui_MYDBSM.h"

MYDBSM::MYDBSM(QWidget *parent)
    : QMainWindow(parent)
{
    ui.setupUi(this);
    //connect(ui->aa, &QPushButton::clicked, this, &MYDBSM::handleClick);

    connect(ui.button_choice, &QPushButton::clicked, this, &MYDBSM::onButtonChoiceClicked);
}


MYDBSM::~MYDBSM()
{}

void MYDBSM::onButtonChoiceClicked()
{
    // 弹出文件选择对话框
    QString filePath = QFileDialog::getOpenFileName(this, "select database", "", "all files (*.*);;database file (*.dbf);;json file (*.json)");

    // 如果文件路径不为空，则显示所选文件的路径
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "文件选择", "您选择的文件是: " + filePath);
        ui.label_root->setText(filePath);
    }
}
