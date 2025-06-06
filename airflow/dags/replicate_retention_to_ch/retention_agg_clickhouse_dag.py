from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator

from clickhouse_driver import Client

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

default_args = {
    'owner': 'artur',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_data_from_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
        SELECT cohort_week, week_0, week_1, week_2, week_3, week_4, week_5
        FROM cdm.retention
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=columns)

    # Сериализуем DataFrame с датой в ISO формате
    json_str = df.to_json(orient='split', date_format='iso')
    kwargs['ti'].xcom_push(key='retention_df', value=json_str)

def insert_data_to_clickhouse(**kwargs):
    ti = kwargs['ti']
    json_df = ti.xcom_pull(key='retention_df', task_ids='fetch_data_from_postgres')
    df = pd.read_json(json_df, orient='split')

    # Явно приводим cohort_week к datetime
    df['cohort_week'] = pd.to_datetime(df['cohort_week'])

    columns_to_fill = ['week_0', 'week_1', 'week_2', 'week_3', 'week_4', 'week_5']
    # Заполняем NaN нулями и приводим к float с округлением
    df[columns_to_fill] = df[columns_to_fill].fillna(0.0).astype(float).round(3)

    conn = BaseHook.get_connection('clickhouse_conn')
    client = Client(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        port=conn.port or 8123,
        database=conn.schema or 'default',
    )

    rows = list(df.itertuples(index=False, name=None))
    client.execute("INSERT INTO bi.retention VALUES", rows)

with DAG(
    'retention_agg_clickhouse_dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['bi', 'clickhouse', 'retention'],
) as dag:

    wait_for_postgres = ExternalTaskSensor(
        task_id='wait_for_retention_agg_postgres',
        external_dag_id='cohort_retention_agg_dag',
        external_task_id='insert_cohort_retention_data',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=600,           # максимум 10 минут на ожидание
        poke_interval=60,      # проверка раз в минуту
        mode='reschedule',     # не держит слот
        soft_fail=True,        # если задача неуспешна — текущая задача будет SKIPPED, а не FAILED
    )

    truncate_data = ClickHouseOperator(
        task_id='truncate_retention_data',
        clickhouse_conn_id='clickhouse_conn',
        sql="TRUNCATE TABLE bi.retention",
    )

    fetch_data = PythonOperator(
        task_id='fetch_data_from_postgres',
        python_callable=fetch_data_from_postgres,
    )

    insert_data = PythonOperator(
        task_id='insert_data_to_clickhouse',
        python_callable=insert_data_to_clickhouse,
    )

    wait_for_postgres >> truncate_data >> fetch_data >> insert_data