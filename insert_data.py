import psycopg2
import requests
import csv
from datetime import datetime

## log in as data production user

conn = psycopg2.connect(dbname="postgres", user="data_production")
cursor = conn.cursor()

## download the dataset csv file from s3 bucket

#r = requests.get('https://dq-content.s3.amazonaws.com/251/storm_data.csv')
#r = requests.get('https://dq-content.s3.amazonaws.com/251/storm_data_additional.csv')

dataset = map(lambda row: row.split(',') , r.text.split('\r\n')[:-1] )

header = next(dataset)


## format row and insert into table

def parseRow(row):
    parsedRow = [row[0]]
    adtime = row[4]
    print(adtime)
    parsedRow.append( datetime(int(row[1]), int(row[2]), int(row[3]), int(adtime[:2]), int(adtime[2:4])) )
    parsedRow += row[5:]
    return parsedRow

for row in dataset:
    parsedData = parseRow(row)
    cursor.execute("INSERT INTO storm_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", parsedData)

conn.commit()
conn.close()