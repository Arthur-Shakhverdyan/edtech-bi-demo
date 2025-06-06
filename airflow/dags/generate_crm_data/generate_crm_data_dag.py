from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging

from generate_crm_data import generate_users, generate_enrollments

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def get_existing_ids_and_history():
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    with hook.get_conn() as conn:
        existing_user_ids = set(pd.read_sql("SELECT id FROM raw.users", conn)["id"])
        existing_enroll_ids = set(pd.read_sql("SELECT id FROM raw.enrollments", conn)["id"])
        enrollments_history_df = pd.read_sql("SELECT user_id, course_id, enrolled_at FROM raw.enrollments", conn)

    return existing_user_ids, existing_enroll_ids, enrollments_history_df

def insert_to_postgres(users_df, enrollments_df):
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    with hook.get_conn() as conn:
        cursor = conn.cursor()

        for _, row in users_df.iterrows():
            cursor.execute("""
                INSERT INTO raw.users (id, registered_at, age, city_id, source_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (row.id, row.registered_at, row.age, row.city_id, row.source_id))

        for _, row in enrollments_df.iterrows():
            cursor.execute("""
                INSERT INTO raw.enrollments (id, user_id, course_id, enrolled_at, payment_status)
                VALUES (%s, %s, %s, %s, %s)
            """, (row.id, row.user_id, row.course_id, row.enrolled_at, row.payment_status))

        conn.commit()

def generate_and_insert(**kwargs):
    execution_date = kwargs["ds"]
    day_offset = (datetime.strptime(execution_date, "%Y-%m-%d").date() - datetime.now().date()).days

    existing_user_ids, existing_enroll_ids, enrollments_history_df = get_existing_ids_and_history()

    hook = PostgresHook(postgres_conn_id='postgres_conn')
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM raw.users")
        max_user_id = cursor.fetchone()[0]
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM raw.enrollments")
        max_enroll_id = cursor.fetchone()[0]

    users_df = generate_users(start_id=max_user_id + 1,
                              day_offset=day_offset,
                              existing_user_ids=existing_user_ids)

    enrollments_df = generate_enrollments(users_df, start_id=max_enroll_id + 1,
                                          day_offset=day_offset,
                                          existing_enroll_ids=existing_enroll_ids,
                                          enrollments_history_df=enrollments_history_df)

    insert_to_postgres(users_df, enrollments_df)

def check_new_data(**kwargs):
    execution_date = kwargs['ds']  # 'YYYY-MM-DD'
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw.users WHERE DATE(registered_at) = %s", (execution_date,))
        users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM raw.enrollments WHERE DATE(enrolled_at) = %s", (execution_date,))
        enrollments = cursor.fetchone()[0]

    # Вернёт True, если данных за execution_date ещё нет
    return users == 0 and enrollments == 0

def update_row_counts():
    logger = logging.getLogger("airflow.task")
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw.users")
        users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM raw.enrollments")
        enrollments = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO cdm.raw_table_row_counts (id, users_row_count, enrollments_row_count)
            VALUES (1, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET users_row_count = EXCLUDED.users_row_count,
                          enrollments_row_count = EXCLUDED.enrollments_row_count
        """, (users, enrollments))
        conn.commit()
        logger.info(f"Row counts updated: users={users}, enrollments={enrollments}")

with DAG(
    dag_id="generate_crm_data_dag",
    default_args=default_args,
    description="Generate CRM data with user and enrollment history",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crm", "generation"]
) as dag:

    check = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=check_new_data
    )

    generate = PythonOperator(
        task_id="generate_and_insert",
        python_callable=generate_and_insert,
        provide_context=True
    )

    update = PythonOperator(
        task_id="update_row_counts",
        python_callable=update_row_counts
    )

    check >> generate >> update