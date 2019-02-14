## sample query using a datetime object as threshold for table slicing

import psycopg2
from datetime import datetime

threshold = datetime(2005,10,25,10,10)
length = 4

conn = psycopg2.connect(dbname="postgres", user="analyst")
cursor = conn.cursor()

# make sure your query variables are contained in an indexed data type e.g. tuple, list
cursor.execute("SELECT * FROM storm_data WHERE date > %s AND shape_leng > %s", [threshold, length])
result = cursor.fetchall()

conn.close()

print(len(result))
    
