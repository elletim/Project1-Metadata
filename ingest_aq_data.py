
import psycopg2

connection = psycopg2.connect("dbname=aq user=postgres")
cursor = connection.cursor()

cursor.execute("SELECT * FROM aq_data;")
records = cursor.fetchall()
