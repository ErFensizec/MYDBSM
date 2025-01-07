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
    // �����ļ�ѡ��Ի���
    QString filePath = QFileDialog::getOpenFileName(this, "select database", "", "all files (*.*);;database file (*.dbf);;json file (*.json)");

    // ����ļ�·����Ϊ�գ�����ʾ��ѡ�ļ���·��
    if (!filePath.isEmpty()) {
        //QMessageBox::information(this, "�ļ�ѡ��", "��ѡ����ļ���: " + filePath);
        ui.label_root->setText(filePath);
    }
}
