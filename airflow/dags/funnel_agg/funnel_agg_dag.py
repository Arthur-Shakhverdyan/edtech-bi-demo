from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from pathlib import Path

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Путь к SQL-файлу
SQL_PATH = Path(__file__).parent / "sql" / "funnel_insert.sql"

with DAG(
    dag_id="funnel_agg_dag",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cdm", "aggregates", "funnel"],
) as dag:

    wait_for_registration_data = ExternalTaskSensor(
        task_id="wait_for_registration_data",
        external_dag_id="registration_agg_dag",
        external_task_id='insert_registration_agg',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=600,           # максимум 10 минут на ожидание
        poke_interval=60,      # проверка раз в минуту
        mode='reschedule',     # не держит слот
        soft_fail=True,        # если задача неуспешна — текущая задача будет SKIPPED, а не FAILED
    )

    truncate_funnel_table = PostgresOperator(
        task_id="truncate_funnel_table",
        postgres_conn_id="postgres_conn",
        sql="TRUNCATE TABLE cdm.funnel;",
    )

    insert_data = PostgresOperator(
        task_id="insert_funnel_data",
        postgres_conn_id="postgres_conn",
        sql=SQL_PATH.read_text(),
    )

    wait_for_registration_data >> truncate_funnel_table >> insert_data