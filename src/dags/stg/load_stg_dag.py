import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from stg.stg_loader import CourierLoader, DeliveryLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def Sprint5_Project_load_stg():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')

    #dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    base_url = 'd5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    headers = {
        'X-Nickname': 'Tsitsurin',
        'X-Cohort': '19',
        'X-Project': 'True',
        'IsProject' : 'True',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    @task(task_id="couriers_load")
    def load_couriers():
        courier_loader = CourierLoader(dwh_pg_connect,headers,base_url)
        courier_loader.courier_load_to_stg()

    @task(task_id="deliveries_load")
    def load_deliveries():
        delivery_loader = DeliveryLoader(dwh_pg_connect,headers,base_url)
        delivery_loader.delivery_load_to_stg()


    courier = load_couriers()

    delivery = load_deliveries()

    courier
    delivery

load_stg_dag = Sprint5_Project_load_stg() 
