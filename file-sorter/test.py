import shutil 
import os
from pathlib import Path
from PySide6.QtWidgets import QFileDialog,QMainWindow,QApplication
from datetime import datetime
def file_sorter(path:Path,date:bool=False):
 extension_data={
     ".jpg":"Pictures",
     ".jpeg":"Pictures",
     ".png":"Pictures",
     ".gif":"Pictures",

     ".mp4":"Videos",
     ".mkv":"Videos",
     ".avi":"Videos",

     ".pdf":"Documents",
     ".txt":"Documents",
     ".docx":"Documents",

     ".mp3":"Music",
     ".wav":"Music",
     ".flac":"Music",}
 
 for file in path.iterdir():
  if file.is_file():
   extension=file.suffix.lower()
   folder=extension_data.get(extension)
   if folder:
    destination=path/folder
    destination.mkdir(exist_ok=True)
    if date:
     creation_time=file.stat().st_birthtime
     year=str(datetime.fromtimestamp(creation_time).year)
     year_folder=destination/year
     year_folder.mkdir(exist_ok=True)
     shutil.move(file,year_folder/file.name)
    else:
     destination.mkdir(exist_ok=True)
     shutil.move(file,destination/file.name)
   else:
     destination=path/"Others"
     destination.mkdir(exist_ok=True)
     shutil.move(file,destination/file.name)

 return path
