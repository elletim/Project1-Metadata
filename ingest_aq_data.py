import psycopg2
import pandas as pd 
import requests as req
from io import StringIO

#/Users/elletimmer/Downloads/Project 1 Metadata/envmeta/bin/activate"
urls = ["http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt", 
        "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/San_Diego.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/New_York/New_York_City.txt",
       "http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt"]
#url = "http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/California/Los_Angeles.txt"

for url in urls:
    page = req.get(url) 
    page_data = page.text
    page_line = page_data.split('\n')
    def get_value(line):
        line= (page_line[line])
        value = line.split(":",1)[1]
        return value
    country =get_value(0)
    city = get_value(1)
    region = get_value(3)
    lat = get_value(6)
    long = get_value(7)
    timezone= get_value(8)
    city_id=  region+city
    print(city_id)
    
connection = psycopg2.connect("dbname=aq user=postgres")
cursor = connection.cursor()
#cursor.execute(" " "
 #INSERT INTO aq_meta (city, country, region, lat, long, timezone, city_id) 
 #VALUES (%s, %s, %s, %s, %s, %s, %s); ())
cursor.execute("SELECT * FROM aq_data;")
records = cursor.fetchall()
#print(records)
