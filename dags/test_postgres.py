from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG('test_postgres_conn', start_date=datetime(2026, 1, 1), schedule_interval=None, catchup=False) as dag:

    @task
    def test():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user;")
        print(cur.fetchone())

    test()