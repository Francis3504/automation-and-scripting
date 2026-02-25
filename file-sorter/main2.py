import sys
from PySide6.QtWidgets import QApplication
from gui import Window
from manager import Controller
def main():
    app=QApplication(sys.argv)
    control=Controller()
    window=Window(control)
    window.show()
    sys.exit(app.exec())

if __name__=="__main__":
    main()
