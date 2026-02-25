from pathlib import Path
from test import file_sorter
class Controller:
    def __init__(self):
        self.folder_path=None

    def set_folder(self,path:Path):
        self.folder=path
        return self.folder
    def sort_files(self,path:Path,date:bool=False):
        sorted_files=file_sorter(path,date)
        return sorted_files   