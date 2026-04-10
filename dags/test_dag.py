from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
from include.webflow_client import WebFlowAPI
import pandas as pd
OUTPUT_FOLDER = "/tmp/webflow_reports"
RAW_DATA_PATH = f"{OUTPUT_FOLDER}/ventas_raw.csv"

default_args = {
    'owner': 'julian',
    'start_date': datetime(2026, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

def extract_webflow_data(**kwargs):
    """
    Tarea que orquestra el login y la bajada del CSV.
    """
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    client = WebFlowAPI(username="TU_USUARIO", password="TU_PASSWORD")
    
    if not client.login():
        raise Exception("Error crítico: No se pudo loguear en WebFlow.")
    
    df = client.get_reporte(id_cliente="260", estado="316")
    
    if df is not None:
        df.to_csv(RAW_DATA_PATH, index=False)
        logging.info(f"Datos guardados exitosamente en {RAW_DATA_PATH}")
    else:
        raise Exception("Error: El reporte no devolvió datos.")

def transform_webflow_data(**kwargs):
    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError("No hay datos crudos para procesar.")
    df = pd.read_csv(RAW_DATA_PATH)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    
    # Guardado final (puedes cambiar a .parquet si querés)
    final_path = RAW_DATA_PATH.replace("_raw.csv", "_clean.csv")
    df.to_csv(final_path, index=False)
    logging.info(f"Reporte transformado y listo en: {final_path}")

# --- Definición del DAG ---

with DAG(
    dag_id='webflow_api_ingestion',
    description='ETL para reportes de WebFlow (CGI Scraping)',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['scraping', 'webflow', 'report']
) as dag:

    extract = PythonOperator(
        task_id='extract_from_webflow',
        python_callable=extract_webflow_data
    )

    transform = PythonOperator(
        task_id='transform_webflow_report',
        python_callable=transform_webflow_data
    )

    extract >> transform