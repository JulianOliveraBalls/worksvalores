from airflow.decorators import dag, task
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import datetime, timedelta
import logging
import os
import pandas as pd

# Mantenemos tus imports de lógica de negocio
from include.inceptia_mappings import (
    map_evento,
    get_operador,
    get_comentario,
    get_estados 
)
from include.inceptia_client import InceptiaAPI

# ==========================================================
# CONFIGURACIÓN DE TEST
# ==========================================================
# Caso Positivo: Poné una fecha donde sepas que hubo llamadas.
# Caso Negativo: Poné una fecha donde sepas que NO hubo (o una futura).
# Una vez probado, cambiaremos ds_to_use por context['ds'].
FECHA_TEST = "2026-04-07" 
MODO_TEST = False # Cambiar a False para usar la fecha automática de Airflow
# ==========================================================

default_args = {
    "owner": "Julián",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="cashea_reports_transvalores",
    start_date=datetime(2026, 1, 1),
    schedule="50 2 * * *", 
    catchup=False,
    default_args=default_args,
    tags=["inceptia", "prod"],
)
def inceptia_dag():

    @task
    def extract(bot_id: int, ds=None):
        """
        Extrae el reporte masivo. 
        Maneja el caso de DF vacío.
        """
        api = InceptiaAPI()
        
        # Lógica de switch para testeo
        ds_to_use = FECHA_TEST if MODO_TEST else ds
        
        logging.info(f"🚀 Extrayendo reporte masivo para: {ds_to_use}")

        try:
            df = api.obtener_reporte_zip(bot_id, ds_to_use, ds_to_use)

            if df is None or df.empty:
                logging.warning(f"⚠️ No hay registros para {ds_to_use}")
                return "NO_DATA"

            output_path = f"/tmp/inceptia_raw_{ds_to_use}.csv"
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            return output_path

        except Exception as e:
            logging.error(f"❌ Error crítico en extract: {e}")
            raise

    @task
    def transform(path_csv: str, ds=None):
        """
        Transformación solo si hay datos.
        Mapea el .zip de Inceptia para subir a Webflow
        """
        ds_to_use = FECHA_TEST if MODO_TEST else ds
        
        if path_csv == "NO_DATA":
            logging.info("Saltando transformación: No hay datos.")
            return "NO_DATA"

        df = pd.read_csv(path_csv)
        logging.info(f"Transformando {len(df)} registros para {ds_to_use}")

        # Lógica de mapeo
        df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.strftime("%d/%m/%Y")
        
        df_final = pd.DataFrame({
            "FECHA": df["Fecha"], 
            "HORA": df["Hora"],
            "OPERADOR": [get_operador() for _ in range(len(df))],
            "CARPETA": df["Codigo"],
            "EVENTO": df["Estado"].apply(map_evento),
            "COMENTARIOS": [
                get_comentario(row["Estado"], i, row["Telefono"]) 
                for i, row in df.iterrows()
            ]
        })
        
        output_xlsx = f"/tmp/inceptia_final_{ds_to_use}.xlsx"
        df_final.to_excel(output_xlsx, index=False)
        return output_xlsx

    @task
    def load(path_xlsx: str, ds=None):
        """
        Envía archivo o alerta de 'Sin datos' a Slack al canal #cashea_reportes
        """
        ds_to_use = FECHA_TEST if MODO_TEST else ds
        slack_hook = SlackHook(slack_conn_id="slack_conn")

        if path_xlsx == "NO_DATA":
            # 📢 ALERTA SIN DATOS
            msg = f"ℹ️ *Reporte Inceptia ({ds_to_use})*: No se encontraron gestiones para subir a Webflow."
            slack_hook.get_conn().chat_postMessage(channel="C0ARL1VRSVC", text=msg)
            logging.info("Notificación de vacíos enviada.")
            return

        slack_hook.get_conn().files_upload_v2(
            channel="C0ARL1VRSVC",
            file=path_xlsx,
            title=f"Reporte Inceptia {ds_to_use}",
            initial_comment=f":bar_chart: *Gestiones automáticas BOT CASHEA* - Fecha: {ds_to_use}"
        )
        logging.info(f"Reporte {ds_to_use} enviado a Slack.")

    data_date = "{{ data_interval_end | ds }}"

    raw_data = extract(bot_id=806, ds=data_date)
    clean_data = transform(raw_data, ds=data_date)
    load(clean_data, ds=data_date)

# Ejecución del DAG
dag_instance = inceptia_dag()