from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import os ,sys
os.path.dirname(sys.executable)
website="https://www.bing.com/ck/a?!&&p=0545b528776b44189f661c9f5634c0d375d140c02d2cf91227287c59674cd671JmltdHM9MTc2NDU0NzIwMA&ptn=3&ver=2&hsh=4&fclid=3676f7b9-01be-60c0-3d91-e46800be61d3&psq=the+sun&u=a1aHR0cHM6Ly93d3cudGhlc3VuLmNvLnVrLw"
service=Service(r"C:\Tools\chromedriver\chromedriver-win64\chromedriver.exe")
driver=webdriver.Chrome(service=service )
driver.get(website)
     