import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from stg.dds_loader import Loader_DDS
log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def Sprint5_Project_load_dds():
    dwh_pg_connect = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')

    @task(task_id="couriers_load_dds")
    def load_couriers_dds():
        courier_dds_loader = Loader_DDS(dwh_pg_connect)
        courier_dds_loader.upload_dds_courier()

    @task(task_id="orders_load_dds")
    def load_orders_dds():
        order_dds_loader = Loader_DDS(dwh_pg_connect)
        order_dds_loader.upload_dds_order()

    @task(task_id="deliveries_load_dds")
    def load_deliveries_dds():
        order_dds_loader = Loader_DDS(dwh_pg_connect)
        order_dds_loader.upload_dds_delivery()

    @task(task_id="fct_load_dds")
    def load_fct_dds():
        order_dds_loader = Loader_DDS(dwh_pg_connect)
        order_dds_loader.upload_dds_fct()

    courier = load_couriers_dds()
    order = load_orders_dds()
    delivery = load_deliveries_dds()
    fct = load_fct_dds()

    courier
    order
    delivery
    fct


load_dds_dag = Sprint5_Project_load_dds() 
