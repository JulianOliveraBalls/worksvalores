# /root/airflow/dags/report_cashea_vps.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Julian',
    'start_date': datetime(2026, 4, 1),
}

with DAG(
    'reporte_cashea_vps_style',
    default_args=default_args,
    schedule_interval='0 9,14,19 * * *',
    catchup=False
) as dag:

    ejecutar_bot = BashOperator(
            task_id='lanzar_playwright_vps',
            bash_command='python3 /opt/airflow/include/script_cashea.py',
            # No hace falta el env={...} si ya está en el docker-compose como SLACK_BOT_TOKEN: ${SLACK_BOT_TOKEN}
        )