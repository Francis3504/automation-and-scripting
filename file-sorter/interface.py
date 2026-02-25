import os
import sys
import tkinter as tk
from colorama import Fore,Style,init
from tkinter import filedialog
from manager import Controller
from pathlib import Path



init(autoreset=True)
class UI:
    def __init__(self,control:Controller):
        self.control=control

    def show(self):
      while True:
        print(Fore.CYAN+"1.Sort Files.")
        print(Fore.CYAN+"2.Exit")
        choice=input(Fore.YELLOW+"\nSelect the options: ").strip()
        if choice=="1":
           self.control_panel()
        elif choice=="2":
           print("Thanks for your time.")
           return 
        else:
           print(Fore.RED+"Invalid input")

    def control_panel(self):
     while True:
       print(Style.BRIGHT+Fore.MAGENTA+"File Sorter v1.\n")
       print(Fore.WHITE+"----------------------------.\n")
       print(Fore.CYAN+"1.Sort by type ")
       print(Fore.CYAN+"2.Sort by year.")
       print(Fore.CYAN+"3.Logout.")
       choice=input(Fore.YELLOW+"\nSelect the options: ")
       if choice=="1":
          self.sort_by_type()
       elif choice=="2":
          self.sort_by_year()
       elif choice=="3":
          print("Logging out.....")
          sys.exit()
       else:
          print(Fore.RED+"Invalid input")

    def sort_by_type(self):
       folder=Path(filedialog.askdirectory())
       if folder:
        path=self.control.set_folder(folder)
        files=self.control.sort_files(path)
        os.startfile(files)
        return
       else:
          return
       
    def sort_by_year(self):
       folder=Path(filedialog.askdirectory())
       if folder:
        path=self.control.set_folder(folder)
        files=self.control.sort_files(path,date=True)
        os.startfile(files)
        return
       else:
          return


