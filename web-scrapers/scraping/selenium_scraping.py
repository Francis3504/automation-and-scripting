from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
options=Options()
options.headless=True
service=Service(r"C:\Tools\chromedriver\chromedriver-win64\chromedriver.exe")#error is here
driver=webdriver.Chrome(service=service,options=options)
print("opening....")
driver.get("Https://google.com")    
print("closing....")      
driver.quit()   