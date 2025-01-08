#pragma once

#include <QtWidgets/QMainWindow>
#include <QPushButton>
#include <QFileDialog>
#include <QMessageBox>
#include "ui_MYDBSM.h"

class MYDBSM : public QMainWindow
{
    Q_OBJECT

public:
    MYDBSM(QWidget *parent = nullptr);
    ~MYDBSM();
private slots:
    void onButtonChoiceClicked();  // �ۺ���
    void refreshTableFromFile();
private:
    Ui::MYDBSMClass ui;
};
