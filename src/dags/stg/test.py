
from datetime import datetime
from logging import Logger as task_logger

import requests

from  psycopg2.extras import execute_values
import psycopg2

nickname = 'Tsitsurin'
cohort = '19'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

base_url = 'd5d04q7d963eapoepsqr.apigw.yandexcloud.net'

headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : api_key,
         }

port = "15432"
host = "localhost"
login = "jovyan"
password = "jovyan"

pg_schema = 'stg'
#pg_table = 'couriers'
pg_table = 'deliveries'

def upload_couriers(pg_schema, pg_table, port, login, host, password):

    offset = 1

    couriers_rep = requests.get(f'https://{base_url}/{pg_table}/?sort_field=_id&sort_direction=asc&offset={offset}',headers = headers).json()

    columns = ','.join([i for i in couriers_rep[0]])
    values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]

    print(f"COLUMNS: {columns}")
    #print(f"VALUES: {values}")
'''
    offset = 0
    while True:    
        couriers_rep = requests.get(f'https://{base_url}/{pg_table}/?sort_field=_id&sort_direction=asc&offset={offset}',
                           headers = headers).json()
        
        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            #task_logger.info(f'Writting {offset} rows')
            break

        
        columns = ','.join([i for i in couriers_rep[0]])
        values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]

        #sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s"
        #execute_values(cursor, sql, values)

        print(f"COLUMNS: {columns}")
        print(f"VALUES: {values}")

        offset += len(couriers_rep)  
'''




upload_couriers(pg_schema, pg_table, port, login, host, password)


