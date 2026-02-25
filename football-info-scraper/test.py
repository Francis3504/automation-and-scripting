import pandas as pd
football=pd.read_csv("https://www.football-data.co.uk/mmz4281/2526/E0.csv")
fg=football.rename(columns={"FTHG":None,"FTAG":"well_fuck"},inplace=True)
print(fg)

