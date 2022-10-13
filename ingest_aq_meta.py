import psycopg2
import pandas as pd 
import requests as req
from io import StringIO

urls = ["http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt", 
        "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Diego.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/New_York/New_York.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt"]

url = "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt"
#for url in urls: 
page = req.get(url) 
page_df= pd.read_csv(StringIO(page.text),skiprows=10,sep='\t')
page_df.columns= ['Year','Month','Day', 'UTC Hour','PM2.5','PM10_mask','Retrospective']     
page_df = page_df[["Year","Month","Day","PM2.5","PM10_mask"]]
df = pd.DataFrame(data=page_df)
df["Date"] = pd.to_datetime(df[["Year","Month","Day"]])
print(df.head())