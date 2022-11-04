import psycopg2
import pandas as pd 
import requests as req
from io import StringIO
import numpy as np


def get_datetime(url):
        page = req.get(url) 
        page_df= pd.read_csv(StringIO(page.text),skiprows=10,sep='\t')
        page_df.columns= ['Year','Month','Day', 'UTC Hour','PM2.5','PM10_mask','Retrospective']     
        page_df = page_df[["Year","Month","Day","UTC Hour"]]
        df = pd.DataFrame(data=page_df)
        cols = df.columns
        d = {"UTC Hour":"hour", "Year":"year", "Month":"month", "Day":"day"}
        print(df['UTC Hour'])
        df["Date"] = pd.to_datetime(df[cols].rename(columns=d))
        datetime = df["Date"].to_list()
        return(datetime)

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
        city = str.strip(city)
        return(city)

def get_cityid(city_data):
        cursor.execute('''SELECT city_id FROM aq_meta WHERE city = (%s)''', (city_data,))
        records = cursor.fetchall()
        return(records)


if __name__ == "__main__":
    urls = ["http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt", 
        "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Diego.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/New_York/New_York_City.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Francisco.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/Colorado/Denver.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/Georgia/Atlanta.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/Texas/Houston.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/Arizona/Phoenix.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/Maryland/Baltimore.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/Maharashtra/Mumbai.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_Arab_Emirates/Dubai/Dubai.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/Bangladesh/Dhaka/Dhaka.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/China/Guangdong/Guangzhou.txt"]
       
    connection = psycopg2.connect("dbname=aq user=postgres")
    cursor = connection.cursor()
    cursor.execute("DROP table IF EXISTS aq_data")
    cursor.execute("CREATE TABLE aq_data (city_id int REFERENCES aq_meta (city_id), datetime timestamp, pm2_5 float);")
    for url in urls:
        datetime = get_datetime(url)
        pm2_5 = get_pm25(url)
        city_data = get_city(url)
        city_id = get_cityid(city_data)
        city_id = city_id[0]
        city_id = int(str(city_id).replace('(', '').replace(')','').replace(',',''))
        #cursor.execute('''SELECT max(datetime) FROM aq_meta WHERE city_id = (%s)''', (city_id,))
        #last_date = datetime[len(datetime)-1]
        #if city_id = 
        for d, p in zip(datetime, pm2_5):
                cursor.execute('''INSERT INTO aq_data (city_id, datetime, pm2_5) VALUES (%s, %s, %s)''', (city_id, d,p))
        cursor.execute("SELECT * FROM aq_data;")
        records = cursor.fetchall()
        #print(records)
        connection.commit()
    cursor.close()
    connection.close()
    

     
        
    


