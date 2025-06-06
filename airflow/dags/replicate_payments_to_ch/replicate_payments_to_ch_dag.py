from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from clickhouse_driver import Client
import psycopg2

default_args = {
    'owner': 'artur',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def get_clickhouse_client():
    conn = BaseHook.get_connection('clickhouse_conn')
    return Client(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        port=conn.port or 8123,
        database=conn.schema or 'default',
    )

def get_two_months_ago_start(execution_date):
    first_of_month = execution_date.replace(day=1)
    two_months_ago = first_of_month - relativedelta(months=2)
    return two_months_ago

def fetch_payments_data(**context):
    execution_date = context['execution_date'].date()
    start_date = get_two_months_ago_start(execution_date)
    print(f"Fetching data from Postgres starting from {start_date}")

    pg_conn = BaseHook.get_connection("postgres_conn")
    conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port,
        user=pg_conn.login,
        password=pg_conn.password,
        dbname=pg_conn.schema
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, course_id, successful_payments, revenue
        FROM cdm.payments
        WHERE date >= %s
    """, (start_date,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    dates = [row[0] for row in results]
    print(f"Fetched {len(results)} rows, date range: {min(dates) if dates else 'N/A'} - {max(dates) if dates else 'N/A'}")

    return results

def delete_old_data_from_clickhouse(**context):
    client = get_clickhouse_client()
    execution_date = context['execution_date'].date()
    start_date = get_two_months_ago_start(execution_date)
    start_date_str = start_date.strftime('%Y-%m-%d')

    print(f"Deleting data in ClickHouse from {start_date_str} onwards")
    client.execute(f"""
        ALTER TABLE bi.payments
        DELETE WHERE date >= '{start_date_str}'
    """)

def insert_into_clickhouse(**context):
    data = context['ti'].xcom_pull(task_ids='fetch_payments_data')
    if not data:
        print("No data to insert into ClickHouse")
        return
    client = get_clickhouse_client()
    client.execute(
        "INSERT INTO bi.payments (date, course_id, successful_payments, revenue) VALUES",
        data
    )
    print(f"Inserted {len(data)} rows into ClickHouse")

with DAG(
    dag_id='payments_agg_clickhouse_dag',
    default_args=default_args,
    description='Replicate daily aggregates from cdm.payments (Postgres) to bi.payments (ClickHouse)',
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=True,
    tags=['clickhouse', 'payments'],
) as dag:

    wait_for_postgres = ExternalTaskSensor(
        task_id='wait_for_payments_agg_postgres',
        external_dag_id='payments_agg_dag',
        external_task_id='insert_payments_agg',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=600,
        poke_interval=60,
        mode='reschedule',
        soft_fail=True
    )

    fetch_data = PythonOperator(
        task_id='fetch_payments_data',
        python_callable=fetch_payments_data,
    )

    delete_old = PythonOperator(
        task_id='delete_old_data_from_clickhouse',
        python_callable=delete_old_data_from_clickhouse,
    )

    insert_data = PythonOperator(
        task_id='insert_into_clickhouse',
        python_callable=insert_into_clickhouse,
    )

    wait_for_postgres >> fetch_data >> delete_old >> insert_data