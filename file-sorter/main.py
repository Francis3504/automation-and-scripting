from interface import UI
from manager import Controller

def main():
    ctrl=Controller()
    app=UI(ctrl)
    app.show()

if __name__=="__main__":
    main()