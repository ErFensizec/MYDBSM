#include "MYDBSM.h"
#include <QtWidgets/QApplication>

int main(int argc, char *argv[])
{
    QApplication a(argc, argv);

    MYDBSM w;

    w.show();
    return a.exec();
}
