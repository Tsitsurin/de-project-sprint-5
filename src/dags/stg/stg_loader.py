from datetime import datetime
from logging import Logger as task_logger

import requests

#from lib import PgConnect
#from lib.dict_util import json2str
from airflow.hooks.base import BaseHook
from  psycopg2.extras import execute_values
import psycopg2

class CourierLoader:
    
    def __init__(self, pg_dest, headers, base_url) -> None:
        self.pg_dest = pg_dest
        self.headers = headers
        self.base_url = base_url


    def courier_load_to_stg(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()
        cur.execute("DELETE FROM stg.couriers;")

        offset = 0
        while True:    
            couriers_rep = requests.get(f'https://{self.base_url}/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',
                                headers = self.headers).json()
            if len(couriers_rep) == 0:
                conn.commit()
                cur.close()
                conn.close()
                print(f'Writting {offset} rows')
                break
            
            columns = ','.join([i for i in couriers_rep[0]])
            values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]
            sql = f"INSERT INTO stg.couriers ({columns})  VALUES %s"
            execute_values(cur, sql, values)
            offset += len(couriers_rep)


class DeliveryLoader:
    def __init__(self, pg_dest, headers, base_url) -> None:
        self.pg_dest = pg_dest
        self.headers = headers
        self.base_url = base_url

    def delivery_load_to_stg(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()
        cur.execute("DELETE FROM stg.deliveries;")

        offset = 0
        while True:    
            deliveries_rep = requests.get(f'https://{self.base_url}/deliveries/?sort_field=_id&sort_direction=asc&offset={offset}',headers = self.headers).json()

            if len(deliveries_rep) == 0:
                conn.commit()
                cur.close()
                conn.close()
                print(f'Writting {offset} rows')
                break

            columns = ','.join([q for q in deliveries_rep[0]])
            values = [[value for value in deliveries_rep[q].values()] for q in range(len(deliveries_rep))]
            sql = f"INSERT INTO stg.deliveries ({columns}) VALUES %s"
            execute_values(cur, sql, values)
            offset += len(deliveries_rep)
