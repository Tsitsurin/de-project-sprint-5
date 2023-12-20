import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from stg.cdm_loader import Loader_CDM
log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def Sprint5_Project_load_cdm():
    dwh_pg_connect = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')

    @task(task_id="load_courier_ledger_cdm")
    def load_lefger_cdm():
        cdm_loader = Loader_CDM(dwh_pg_connect)
        cdm_loader.upload_cdm()

    ledger = load_lefger_cdm()


    ledger


load_dds_dag = Sprint5_Project_load_cdm() 
