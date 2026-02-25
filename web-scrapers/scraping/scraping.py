import pandas as pd
import requests
from io import StringIO

url="https://en.wikipedia.org/wiki/List_of_The_Simpsons_episodes"
headers={"User-Agent":"Mozilla/5.0(Windows NT 10.0; Win64; x64)"}
html=requests.get(url,headers=headers).text
simpsons=pd.read_html(StringIO(html))
print(simpsons)