from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta, date
import logging

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_new_user_data():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(date) FROM cdm.registration")
    last_agg_date = cursor.fetchone()[0]
    if last_agg_date is None:
        last_agg_date = date(2000, 1, 1)
        
    logging.info(f"last_agg_date = {last_agg_date}")

    cursor.execute(
        "SELECT MIN(registered_at::date) FROM raw.users WHERE registered_at::date > %s",
        (last_agg_date,)
    )
    new_date = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    logging.info(f"last_agg_date: {last_agg_date}, new_date: {new_date}")

    return new_date is not None

def insert_registration_agg():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO cdm.registration (date, city_id, source_id, age_group, users_count)
    SELECT u.registered_at::date AS date,
           u.city_id,
           u.source_id,
           CASE
               WHEN u.age < 18 THEN '<18'
               WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
               WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
               WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
               WHEN u.age >= 45 THEN '45+'
               ELSE 'Unknown'
               END               AS age_group,
           COUNT(*)              AS users_count
    FROM raw.users u
    WHERE u.registered_at::date > (SELECT COALESCE(MAX(date), '2000-01-01') FROM cdm.registration)
    GROUP BY u.registered_at::date,
             u.city_id,
             u.source_id,
             age_group;
    """

    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="registration_agg_dag",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cdm", "aggregates", "registrations"]
) as dag:

    wait_for_generate_crm_data = ExternalTaskSensor(
    task_id="wait_for_generate_crm_data",
    external_dag_id="generate_crm_data_dag",
    external_task_id='update_row_counts',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    timeout=600,           # максимум 10 минут на ожидание
    poke_interval=60,      # проверка раз в минуту
    mode='reschedule',     # не держит слот
    soft_fail=True,        # если задача неуспешна — текущая задача будет SKIPPED, а не FAILED
    )

    check_data = ShortCircuitOperator(
        task_id="check_new_user_data",
        python_callable=check_new_user_data,
    )

    insert_data = PythonOperator(
        task_id="insert_registration_agg",
        python_callable=insert_registration_agg,
    )

    wait_for_generate_crm_data >> check_data >> insert_data