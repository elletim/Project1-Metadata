import psycopg2
import pandas as pd 
import requests as req
from io import StringIO
#/Users/elletimmer/Downloads/Project 1 Metadata/envmeta/bin/activate"


def get_value(page_line, line_num):
        line= (page_line[line_num])
        value = line.split(":",1)[1]
        return value

def get_meta(urls):
    meta = []
    for indx, url in enumerate(urls):
        page = req.get(url) 
        page_data = page.text
        page_line = page_data.split('\n')
        country =get_value(page_line,0).strip().replace("\r", "")
        city = get_value(page_line, 1).strip().replace("\r", "")
        region = get_value(page_line,3).strip().replace("\r", "")
        lat = float(get_value(page_line,6))
        long = float(get_value(page_line,7))
        timezone= get_value(page_line,8).strip().replace("\r", "")
        city_id = indx
        meta.append([city, country, region, lat, long, timezone, city_id])
    return(meta)

if __name__ == "__main__":
    urls = ["http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt", 
        "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Diego.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/New_York/New_York_City.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt"]

    meta = get_meta(urls)
    
    connection = psycopg2.connect("dbname=aq user=postgres")
    cursor = connection.cursor()
    cursor.execute("DROP table IF EXISTS aq_meta")
    cursor.execute("CREATE TABLE aq_meta (city varchar(50), country varchar(50), region varchar(50), lat float, long float, time_zone varchar(50), city_id int, PRIMARY KEY(city_id));")
    for city,country,region, lat, long, timezone, city_id in meta:

        cursor.execute('''INSERT INTO aq_meta (city, country, region, lat, long, time_zone, city_id) VALUES (%s, %s, %s, %s, %s, %s, %s)''', (city, country, region, lat, long, timezone, city_id))
        cursor.execute("SELECT * FROM aq_meta;")
        records = cursor.fetchall()
        print(records)
        connection.commit()
    
    cursor.close()
    connection.close()
