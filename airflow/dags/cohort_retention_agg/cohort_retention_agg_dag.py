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
SQL_PATH = Path(__file__).parent / "sql" / "insert_retention.sql"

with DAG(
    dag_id="cohort_retention_agg_dag",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cdm", "aggregates", "retention"],
) as dag:

    wait_for_payments_data = ExternalTaskSensor(
        task_id="wait_for_payments_data",
        external_dag_id="payments_agg_dag",
        external_task_id='insert_payments_agg',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=600,           # максимум 10 минут на ожидание
        poke_interval=60,      # проверка раз в минуту
        mode='reschedule',     # не держит слот
        soft_fail=True,        # если задача неуспешна — текущая задача будет SKIPPED, а не FAILED
    )

    truncate_table = PostgresOperator(
        task_id="truncate_retention_table",
        postgres_conn_id="postgres_conn",
        sql="TRUNCATE TABLE cdm.retention;",
    )

    insert_data = PostgresOperator(
        task_id="insert_cohort_retention_data",
        postgres_conn_id="postgres_conn",
        sql=SQL_PATH.read_text(),  # читаем SQL из файла одной строкой
    )

    wait_for_payments_data >> truncate_table >> insert_data