import pandas as pd
import random
from pathlib import Path
from faker import Faker
import os
print(os.getcwd())


fake=Faker()
storage=Path(r"C:\Users\tradelist\programming\automation and scripting\csv-merger\seed_data")
storage.mkdir(parents=True,exist_ok=True)

products=["Shoes","Pants","Laptops","Phone","Guns","Pots"]
regions=["Lusaka","Chipata","Vubwi","Sinda","Sam"]

for i in range(1,25):
    row=[]
    for _ in range(10000):
        quantity=random.randint(1,10)
        unit_price=random.randint(50,2000)
        row.append({"invoice_id":fake.uuid4(),"date":fake.date_between(start_date="-1y",end_date="today"),
        "store_id":f"S-{random.randint(1,5)}","product":random.choice(products),"quantity":quantity,"unit_price":unit_price,
        "total":quantity*unit_price,"regions":random.choice(regions)})
    df=pd.DataFrame(row)
    df.to_csv(storage/f"sales_data_{i}_{fake.date_between(start_date="-356d",end_date="today")}.csv",index=False)
    