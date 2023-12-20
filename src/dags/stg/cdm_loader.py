from datetime import datetime
from logging import Logger as task_logger

import requests

from airflow.hooks.base import BaseHook
from  psycopg2.extras import execute_values
import psycopg2

class Loader_CDM:
    
    def __init__(self, pg_dest) -> None:
        self.pg_dest = pg_dest

    def upload_cdm(self) -> None:
        conn = psycopg2.connect(f"dbname='de' port='{self.pg_dest.port}' user='{self.pg_dest.login}' host='{self.pg_dest.host}' password='{self.pg_dest.password}'")        
        cur = conn.cursor()

        sql_d = '''
                delete from 
                    cdm.dm_courier_ledger 
                where 
                        settlement_year = (select extract(year from order_ts) from dds.fct_deliveries)
                    and settlement_month = (select extract(month from order_ts) from dds.fct_deliveries);
                '''
        
        cur.execute(sql_d)

        sql =   """
                INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                with fact as (
                    select 
                        case 
                            when f.rate_avg < 4 then case when fd.total_sum*0.05 < 100 then 100 else fd.total_sum*0.05 end 
                            when f.rate_avg >=4 and f.rate_avg < 4.5 then case when fd.total_sum*0.07 < 150 then 150 else fd.total_sum*0.07 end 
                            when f.rate_avg >= 4.5 and f.rate_avg < 4.9 then case when fd.total_sum*0.08 < 175 then 175 else fd.total_sum*0.08 end
                            when f.rate_avg >= 4.9 then case when fd.total_sum*0.10 < 200 then 200 else fd.total_sum*0.1 end
                        end as courier_order_cash,
                        fd.*,
                        extract(year from fd.order_ts) as settlement_year,
                        extract(month from fd.order_ts) as settlement_month,
                        f.rate_avg
                    from 
                        dds.fct_deliveries fd
                    join (
                            select 
                                fct.courier_id_dwh,
                                extract(year from fct.order_ts) as settlement_year,
                                extract(month from fct.order_ts) as settlement_month,
                                avg(fct.rate)::numeric(10,2) as rate_avg
                            from
                            dds.fct_deliveries fct
                            group by
                                fct.courier_id_dwh,
                                extract(year from fct.order_ts),
                                extract(month from fct.order_ts)
                    ) f on fd.courier_id_dwh = f.courier_id_dwh and extract(year from fd.order_ts) = f.settlement_year and extract(month from fd.order_ts) = settlement_month
                )
                select 
                    fct.courier_id_dwh as courier_id,
                    c.name as courier_name,
                    extract(year from fct.order_ts) as settlement_year,
                    extract(month from fct.order_ts) as settlement_month,
                    count(distinct fct.order_id_dwh) as orders_count,
                    sum(fct.total_sum) as orders_total_sum,
                    avg(fct.rate) as rate_avg,
                    (sum(fct.total_sum)/100*25) as order_processing_fee,
                    sum(courier_order_cash) as courier_order_sum,
                    sum(fct.tip_sum) as courier_tips_sum,
                    (sum(courier_order_cash)+sum(fct.tip_sum))*0.95 as courier_reward_sum
                from
                    dds.fct_deliveries fct
                join 
                    dds.dm_couriers c on fct.courier_id_dwh = c.courier_id_dwh
                join 
                    fact f on f.courier_id_dwh = fct.courier_id_dwh
                group by
                    fct.courier_id_dwh,
                    c.name,
                    extract(year from fct.order_ts),
                    extract(month from fct.order_ts);
                """
        cur.execute(sql)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Script: {sql}")
