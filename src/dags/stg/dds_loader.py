from datetime import datetime
from logging import Logger as task_logger

import requests

from airflow.hooks.base import BaseHook
from  psycopg2.extras import execute_values
import psycopg2

class Loader_DDS:
    
    def __init__(self, pg_dest) -> None:
        self.pg_dest = pg_dest

    def upload_dds_courier(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()

        sql =   """
                insert into dds.dm_couriers(courier_id_source,name)
                select distinct
                    stg_c._id as courier_id_source, 
                    stg_c.name
                from 
                    stg.couriers stg_c
                where not exists (
                                    select 
                                        courier_id_source,
                                        name
                                    from 
                                        dds.dm_couriers dds_c
                                    where stg_c._id = dds_c.courier_id_source
                                );
                """
        cur.execute(sql)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Script: {sql}")



    def upload_dds_order(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()

        sql =   """
                insert into dds.dm_orders(order_id_source)
                select distinct
                    order_id as order_id_source
                from 
                    stg.deliveries stg_o
                where not exists (	
                                    select 
                                        order_id_source
                                    from 
                                        dds.dm_orders dds_o
                                    where stg_o.order_id = dds_o.order_id_source
                                );
                """
        cur.execute(sql)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Script: {sql}")


    def upload_dds_delivery(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()

        sql =    """
                insert into dds.dm_deliveries(delivery_id_source)
                select distinct
                    stg_d.delivery_id as delivery_id_source
                from
                    stg.deliveries stg_d
                where not exists (
                                    select 
                                        delivery_id_source
                                    from 
                                        dds.dm_deliveries dds_d
                                    where stg_d.delivery_id = dds_d.delivery_id_source
                                );
                """
        cur.execute(sql)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Script: {sql}")

    def upload_dds_fct(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()

        sql_d = "delete from dds.fct_deliveries where order_ts in (select order_ts from stg.deliveries);"
        
        cur.execute(sql_d)


        sql =   """
                INSERT INTO dds.fct_deliveries (order_id_dwh, delivery_id_dwh, courier_id_dwh, order_ts, delivery_ts, address, rate, tip_sum, total_sum)
                select
                    dds_d.delivery_id_dwh,
                    dds_o.order_id_dwh,
                    dds_c.courier_id_dwh,
                    stg_d.order_ts,
                    stg_d.delivery_ts,
                    stg_d.address,
                    stg_d.rate,
                    stg_d.tip_sum,
                    stg_d.sum as total_sum
                from 
                    stg.deliveries stg_d
                join
                    dds.dm_deliveries dds_d on stg_d.delivery_id = dds_d.delivery_id_source 
                join
                    dds.dm_orders dds_o on stg_d.order_id = dds_o.order_id_source
                join
                    dds.dm_couriers dds_c on stg_d.courier_id = dds_c.courier_id_source;
                """
        cur.execute(sql)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Script: {sql}")
