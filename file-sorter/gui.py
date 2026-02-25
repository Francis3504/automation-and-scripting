import os
from PySide6.QtWidgets import (QMainWindow,QPushButton,QLabel,QLineEdit,QWidget,QMessageBox,
QVBoxLayout,QHBoxLayout,QCheckBox,QFileDialog)
from pathlib import Path
from manager import Controller
class Window(QMainWindow):
    def __init__(self,control:Controller):
        super().__init__()
        self.control=control
        self.setWindowTitle("File Sorter v1")
        self.resize(900,600)
        self.setup_ui()

    def setup_ui(self):
        central=QWidget()
        self.setCentralWidget(central)
        main_box=QVBoxLayout(central)

        box1=QHBoxLayout()
        self.browse=QPushButton("Browse")
        self.browse.setStyleSheet("background-color:blue; color:white;")
        self.browse.setFixedWidth(200)
        self.folder_selected=QLineEdit()
        box1.addWidget(self.browse)
        box1.addWidget(self.folder_selected)
        main_box.addLayout(box1)

        box2=QHBoxLayout()
        choice=QLabel("Sort by:")
        self.type=QCheckBox()
        sort_type=QLabel("File Type")
        self.year=QCheckBox()
        sort_year=QLabel("File Year")

        box2.addWidget(choice)
        box2.addWidget(self.type)
        box2.addWidget(sort_type)
        box2.addWidget(self.year)
        box2.addWidget(sort_year)
        box2.addStretch()
        main_box.addLayout(box2)

        self.sort_files=QPushButton("Sort Files.")
        self.sort_files.setStyleSheet('background-color:blue; color:white;')
        self.sort_files.setFixedWidth(200)
        main_box.addWidget(self.sort_files)

        self.browse.clicked.connect(self.folder)
        self.sort_files.clicked.connect(self.sort)

    def folder(self):
        folder=QFileDialog.getExistingDirectory(self,"Select Folder")
        if folder:
            self.folder_selected.setText(folder)
            self.control.set_folder(Path(folder))
            return
        else:
            return
        
    def sort(self):
        if self.type.isChecked() or self.year.isChecked():
            if self.type.isChecked() and self.year.isChecked():
                QMessageBox.warning(self,"Both boxes ticked","Please only pick one.")
                return
            else:
                path=self.control.folder
                self.control.sort_files(path,date=self.year.isChecked())
                os.startfile(path)



        
        