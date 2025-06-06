from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator

from clickhouse_driver import Client

from datetime import datetime, timedelta
import pandas as pd


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
        SELECT date, city_id, source_id, age_group, users_count
        FROM cdm.registration
        WHERE date >= date_trunc('month', current_date - interval '2 month')
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    import pandas as pd
    df = pd.DataFrame(rows, columns=columns)

    kwargs['ti'].xcom_push(key='registration_df', value=df.to_json(orient='split'))


def insert_data_to_clickhouse(**kwargs):
    ti = kwargs['ti']
    json_df = ti.xcom_pull(key='registration_df', task_ids='fetch_data_from_postgres')
    df = pd.read_json(json_df, orient='split')

    conn = BaseHook.get_connection('clickhouse_conn')
    client = Client(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        port=conn.port or 8123,
        database=conn.schema or 'localnost',
    )

    data = [tuple(x) for x in df.to_numpy()]

    client.execute(
        'INSERT INTO bi.registration (date, city_id, source_id, age_group, users_count) VALUES',
        data
    )


with DAG(
    'registration_agg_clickhouse_dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['bi', 'clickhouse', 'registration'],
) as dag:

    def get_previous_day(execution_date):
        return execution_date - timedelta(days=1)

    wait_for_postgres = ExternalTaskSensor(
    task_id='wait_for_registration_agg_postgres',
    external_dag_id='registration_agg_dag',
    external_task_id='insert_registration_agg',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    timeout=600,           # максимум 10 минут на ожидание
    poke_interval=60,      # проверка раз в минуту
    mode='reschedule',     # не держит слот
    soft_fail=True,        # если задача неуспешна — текущая задача будет SKIPPED, а не FAILED
    execution_date_fn=get_previous_day,
    )

    delete_old = ClickHouseOperator(
        task_id='delete_old_registration_data',
        clickhouse_conn_id='clickhouse_conn',
        sql="""
            ALTER TABLE bi.registration
            DELETE WHERE date >= toStartOfMonth(addMonths(today(), -2))
        """,
    )

    fetch_data = PythonOperator(
        task_id='fetch_data_from_postgres',
        python_callable=fetch_data_from_postgres,
    )

    insert_data = PythonOperator(
        task_id='insert_data_to_clickhouse',
        python_callable=insert_data_to_clickhouse,
    )

    wait_for_postgres >> delete_old >> fetch_data >> insert_data