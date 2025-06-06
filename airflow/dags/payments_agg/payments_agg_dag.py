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

def check_new_payments_data():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(date) FROM cdm.payments")
    last_agg_date = cursor.fetchone()[0]
    if last_agg_date is None:
        last_agg_date = date(2000, 1, 1)

    logging.info(f"last_agg_date = {last_agg_date}")

    cursor.execute(
        "SELECT MIN(enrolled_at::date) FROM raw.enrollments WHERE enrolled_at::date > %s",
        (last_agg_date,),
    )
    new_date = cursor.fetchone()[0]

    logging.info(f"new_date = {new_date}")

    cursor.close()
    conn.close()

    return new_date is not None


def insert_payments_agg():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO cdm.payments (date, course_id, successful_payments, revenue)
       SELECT ca.calendar_date AS date,
              e.course_id,
              COUNT(*) FILTER (WHERE e.payment_status = 'paid') AS successful_payments,
              COALESCE(SUM(c.price) FILTER (WHERE e.payment_status = 'paid'), 0) AS revenue
       FROM dim.calendar ca
       LEFT JOIN (
           SELECT *
           FROM raw.enrollments
           WHERE payment_status = 'paid'
       ) e ON ca.calendar_date = e.enrolled_at::date
       LEFT JOIN dim.courses c ON e.course_id = c.id
       WHERE ca.calendar_date >= (
           SELECT COALESCE(MAX(date), '2000-01-01') FROM cdm.payments
       )
       GROUP BY ca.calendar_date, e.course_id
       ORDER BY ca.calendar_date;
    """

    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="payments_agg_dag",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cdm", "aggregates", "payments"]
) as dag:

    wait_for_generate_crm_data = ExternalTaskSensor(
        task_id="wait_for_generate_crm_data",
        external_dag_id="generate_crm_data_dag",
        external_task_id='update_row_counts',  # ждём завершения таски DAG generate_crm_data
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=600,           # максимум 10 минут на ожидание
        poke_interval=60,      # проверка раз в минуту
        mode='reschedule',     # не держит слот
        soft_fail=True,        # если задача неуспешна — текущая задача будет SKIPPED, а не FAILED
    )

    check_data = ShortCircuitOperator(
        task_id="check_new_payments_data",
        python_callable=check_new_payments_data,
    )

    insert_data = PythonOperator(
        task_id="insert_payments_agg",
        python_callable=insert_payments_agg,
    )

    wait_for_generate_crm_data >> check_data >> insert_data
