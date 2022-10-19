import psycopg2
import pandas as pd 
import requests as req
from io import StringIO
import numpy as np


def get_datetime(url):
        page = req.get(url) 
        page_df= pd.read_csv(StringIO(page.text),skiprows=10,sep='\t')
        page_df.columns= ['Year','Month','Day', 'UTC Hour','PM2.5','PM10_mask','Retrospective']     
        page_df = page_df[["Year","Month","Day","PM2.5",]]
        df = pd.DataFrame(data=page_df)
        df["Date"] = pd.to_datetime(df[["Year","Month","Day"]])
        datetime = df["Date"].to_list()
        return(datetime)
       # data.append([pm2_5, datetime])
       # return(data)

def get_pm25(url):
        page = req.get(url) 
        page_df= pd.read_csv(StringIO(page.text),skiprows=10,sep='\t')
        page_df.columns= ['Year','Month','Day', 'UTC Hour','PM2.5','PM10_mask','Retrospective']     
        page_df = page_df[["Year","Month","Day","PM2.5",]]
        df = pd.DataFrame(data=page_df)
        pm2_5 = df["PM2.5"].to_list()
        return(pm2_5)


def get_city(url):
        page = req.get(url) 
        page_data = page.text
        page_line = page_data.split('\n')
        line= page_line[1]
        city = line.split(":",1)[1]
        return(city)

if __name__ == "__main__":
    urls = ["http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt", 
        "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Diego.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/New_York/New_York_City.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt"]
    
    connection = psycopg2.connect("dbname=aq user=postgres")
    cursor = connection.cursor()
    cursor.execute("DROP table IF EXISTS aq_data")
    cursor.execute("CREATE TABLE aq_data (city_id int REFERENCES aq_meta (city_id), datetime timestamp, pm2_5 float);")
    #cursor.execute("CREATE TABLE aq_data (datetime timestamp[], pm2_5 float[]);")
    for url in urls:
        datetime = get_datetime(url)
        pm2_5 = get_pm25(url)
        city_data = get_city(url)
        #for d, p in (datetime, pm2_5):
                #datetime = d
                #print(datetime)
                #cursor.execute('''INSERT INTO aq_data (datetime) VALUES (%s)''', [d])
        for p in pm2_5:
                print(p)
                #cursor.execute('''INSERT INTO aq_data (pm2_5) VALUES (%s)''', [p])

        #city_id = cursor.execute('''SELECT city_id FROM aq_meta WHERE city = %s''' %city_data)
        city_id = cursor.execute('''SELECT city_id FROM aq_meta WHERE city ='city_data,' ''')
        #cursor.execute('''SELECT city FROM aq_meta ''')
        #cursor.execute('''INSERT INTO aq_data (city_id) VALUE (%s)''', (city_id))
        #cursor.execute('''INSERT INTO aq_data (city_id, datetime, pm2_5) VALUES (%s, %s, %s)''', (city_id, datetime, pm2_5))
        #cursor.execute("SELECT * FROM aq_data;")
        #records = cursor.fetchall()
        #print(records)

     
        
    
      #cursor.close()
      #connection.close()



