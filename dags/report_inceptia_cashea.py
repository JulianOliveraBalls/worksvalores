from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
import pandas as pd
import time
from include.inceptia_mappings import (
    map_evento,
    get_operador,
    get_comentario,
    get_estados 
)
from include.inceptia_client import InceptiaAPI
from slack_sdk import WebClient

OUTPUT_CSV = "/tmp/inceptia_casos.csv"
OUTPUT_XLSX = "/tmp/inceptia_casos.xlsx"

def extract_inceptia(bot_id, estados, **context):
    # fecha = context["ds"]
    fecha_inicio = "2026-04-07"
    fecha_fin = "2026-04-08"

    api = InceptiaAPI()
    all_dfs = []

    logging.info(f"📅 Ejecutando extracción desde {fecha_inicio} hasta {fecha_fin}")

    for i, estado in enumerate(estados):
        try:
            logging.info(f"[{i+1}/{len(estados)}] Estado: {estado}")

            df_temp = api.obtener_todos_los_casos(
                bot_id,
                fecha_inicio,
                fecha_fin,
                estado
            )

            if not df_temp.empty:
                all_dfs.append(df_temp)
                logging.info(f"✔ {len(df_temp)} registros")
            else:
                logging.info("Sin datos")

            if i < len(estados) - 1:
                time.sleep(10)

        except Exception as e:
            logging.error(f"Error en estado '{estado}': {e}")
            continue

    if not all_dfs:
        raise Exception("No se recolectaron datos")

    df_final = pd.concat(all_dfs, ignore_index=True)
    df_final.to_csv(OUTPUT_CSV, index=False)

    logging.info(f"Archivo guardado: {OUTPUT_CSV}")

def transform_inceptia():
    df = pd.read_csv(OUTPUT_CSV)

    logging.info(f"Transformando {len(df)} registros")

    df["last_updated"] = pd.to_datetime(df["last_updated"])
    col_tel = "phone"

    df_final = pd.DataFrame({
        "FECHA": df["last_updated"].dt.strftime("%m/%d/%Y"),
        "HORA": df["last_updated"].dt.strftime("%H:%M:%S"),
        "OPERADOR": [get_operador() for _ in range(len(df))],
        "CARPETA": df["params.code"],
        "EVENTO": df["case_result.name"].apply(map_evento),
        "COMENTARIO": [
            get_comentario(row["case_result.name"], i, row[col_tel])
            for i, row in df.iterrows()
        ]
    })
    
    df_final.to_excel(OUTPUT_XLSX, index=False)

    logging.info("Transformación completa")


def load_inceptia(**context):
    fecha_inicio = "2026-04-07"
    fecha_fin = "2026-04-08"

    client = WebClient(
        token=os.getenv("SLACK_BOT_TOKEN")      )

    try:
        response = client.files_upload_v2(
            channel="C0ARL1VRSVC",
            file=OUTPUT_XLSX,
            title=f"Reporte Inceptia {fecha_inicio} - {fecha_fin}",
            initial_comment=f":bar_chart: *Gestiones automáticas BOT CASHEA - {fecha_inicio} - {fecha_fin}*"
        )

        logging.info("Archivo enviado a Slack correctamente")

    except Exception as e:
        logging.error(f"Error enviando a Slack: {e}")
        raise


with DAG(
    dag_id="inceptia_api_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["inceptia"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_inceptia,
        op_kwargs={
            "bot_id": 806,
            "estados": get_estados()
        },
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_inceptia,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_inceptia,
    )

    extract >> transform >> load