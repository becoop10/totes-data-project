import datetime
import pandas as pd
import math
import psycopg2

host = ''
username = ''
password = ''
database = ''
port = 0

conn = psycopg2.connect(
    host = host,
    database = database,
    user = username,
    password = password,
    port = port
)

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

def write_to_db(query, var_in):
    with conn.cursor() as cur:
        try:
            cur.execute(query, var_in)
            conn.commit()
            cur.close()
        except Exception as e:
            # logger.error('An error occured.')
            print(e)
            raise Exception()

def write_table():
    write_to_db('DELETE FROM dim_date;', ('',))
    for d in pd.date_range(start='11/03/2022', end='11/03/2024'):
        query = 'INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
        values = (
            d,
            d.year,
            d.month,
            d.day,
            d.isoweekday(),
            days[d.weekday()],
            months[d.month - 1],
            math.ceil(d.month / 3 )
        )
        write_to_db(query, values)

write_table()
