import psycopg2

## set up table in database

conn = psycopg2.connect(dbname="postgres", user="postgres")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS storm_data")
cursor.execute("""CREATE TABLE storm_data(
    fid integer PRIMARY KEY,
    date timestamp,
    btid smallint,
    name text,
    lat decimal(3,1),
    long decimal(4,1),
    wind_kts smallint,
    pressure smallint,
    cat varchar(2),
    basin varchar(15),
    shape_leng decimal(8,6)
)""")

## create users (production user for table read/write and analyst for read-only)

cursor.execute("DROP USER IF EXISTS data_production")
cursor.execute("DROP USER IF EXISTS analyst")

cursor.execute('CREATE USER data_production WITH NOSUPERUSER')
cursor.execute('REVOKE ALL ON storm_data FROM data_production')
cursor.execute('GRANT SELECT ON storm_data TO data_production')
cursor.execute('GRANT INSERT ON storm_data TO data_production')
cursor.execute('GRANT UPDATE ON storm_data TO data_production')

cursor.execute('CREATE USER analyst WITH NOSUPERUSER')
cursor.execute('REVOKE ALL ON storm_data FROM analyst')
cursor.execute('GRANT SELECT ON storm_data TO analyst')

conn.commit()
conn.close()