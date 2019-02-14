import psycopg2
import requests
import csv
from datetime import datetime

## log in as data production user

conn = psycopg2.connect(dbname="postgres", user="postgres")
cursor = conn.cursor()

## download the dataset csv file from s3 bucket

r = requests.get('https://dq-content.s3.amazonaws.com/251/storm_data_additional.csv')

dataset = map(lambda row: row.split(',') , r.text.split('\r\n')[:-1] )

header = next(dataset)


## change type of FID into VARCHAR(32)
cursor.execute('ALTER TABLE storm_data ALTER fid TYPE VARCHAR(32)')

## insert into table

for row in dataset:
    cursor.execute("INSERT INTO storm_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)

conn.commit()
conn.close()