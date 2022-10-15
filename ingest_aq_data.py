import psycopg2
import pandas as pd 
import requests as req
from io import StringIO


def get_values(url):
        page = req.get(url) 
        page_df= pd.read_csv(StringIO(page.text),skiprows=10,sep='\t')
        page_df.columns= ['Year','Month','Day', 'UTC Hour','PM2.5','PM10_mask','Retrospective']     
        page_df = page_df[["Year","Month","Day","PM2.5",]]
        df = pd.DataFrame(data=page_df)
        df["Date"] = pd.to_datetime(df[["Year","Month","Day"]])
        pm2_5 = df["PM2.5"].to_list()
        datetime = df["Date"].val
        return(datetime)
def get_city(url):
        page = req.get(url) 
        page_data = page.text
        page_line = page_data.split('\n')
        line= page_line[1]
        city = line.split(":",1)[1]
        return(city)


urls = ["http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt", 
        "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Diego.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/New_York/New_York.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt"]
for url in urls:
        city_data = get_city(url)
       # print(get_values(url))
        connection = psycopg2.connect("dbname=aq user=postgres")
        cursor = connection.cursor()
        cursor.execute("DROP table IF EXISTS aq_data")
        city_id = cursor.execute('''SELECT city_id FROM aq_meta WHERE city =%s''' %city_data)
        print(city_id)
        cursor.execute("CREATE TABLE aq_data (city_id int REFERENCES aq_meta (city_id), datetime timestamp, pm2_5 float);")
        #cursor.execute('''INSERT INTO aq_meta (city_id, country, datetime, pm2_5) VALUES (%s, %s, %s)''', (city_id, dateime, pm2_5))
#cursor.execute("SELECT * FROM aq_meta;")
#records = cursor.fetchall()
#print(records)
#connection.commit()
    
#cursor.close()
#connection.close()



