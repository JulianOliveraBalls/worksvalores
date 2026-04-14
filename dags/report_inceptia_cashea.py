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
    get_comentario
)
from include.inceptia_client import InceptiaAPI

BOT_ID_CASHEA = 806

# Configuración de TEST/PROD
FECHA_TEST = "2026-04-07" 
MODO_TEST = False 

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
    tags=["inceptia", "prod", "data-ops"],
)
def inceptia_dag():

    @task
    def extract(bot_id: int, ds=None):
        """
        Extrae el reporte masivo tras validar existencia de datos.
        Optimización: Usa un pre-check rápido para evitar timeouts en días sin datos.
        """
        api = InceptiaAPI()
        ds_to_use = FECHA_TEST if MODO_TEST else ds
        
        logging.info(f"🚀 Iniciando extracción para: {ds_to_use}")

        try:
            # 1. PRE-CHECK: Verificamos estados de alta frecuencia (Ocupado / Correo de Voz)
            # Esto evita que el Bulk Export (ZIP) se quede en 'IN_PROCESS' infinitamente si no hay nada.
            logging.info("🔍 Ejecutando pre-check de estados frecuentes...")
            
            tiene_datos = False
            # Consultamos el endpoint paginado con un page_size mínimo
            for estado in ["Ocupado", "Correo de Voz"]:
                # Reutilizamos tu lógica de paginado pero solo para la primera página
                df_check = api.obtener_todos_los_casos(
                    bot_id, ds_to_use, ds_to_use, estado=estado
                )
                if not df_check.empty:
                    logging.info(f"✅ Se detectaron gestiones ('{estado}'). Procediendo al reporte pesado.")
                    tiene_datos = True
                    break

            if not tiene_datos:
                logging.warning(f"终止: No se encontraron gestiones base para {ds_to_use}. Abortando ZIP.")
                return "NO_DATA"

            # 2. EXTRACCIÓN PESADA: Solo si el pre-check fue exitoso
            logging.info("📦 Solicitando exportación masiva (ZIP)...")
            df = api.obtener_reporte_zip(bot_id, ds_to_use, ds_to_use)

            if df is None or df.empty:
                logging.warning(f"⚠️ El ZIP final no contenía registros para {ds_to_use}")
                return "NO_DATA"

            output_path = f"/tmp/inceptia_raw_{ds_to_use}.csv"
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            return output_path

        except Exception as e:
            logging.error(f"❌ Error crítico en extract: {e}")
            raise

    @task
    def transform(path_csv: str, ds=None):
        """Mapea el reporte para el formato final de Transvalores."""
        ds_to_use = FECHA_TEST if MODO_TEST else ds
        
        if path_csv == "NO_DATA":
            logging.info("Saltando transformación: No hay datos.")
            return "NO_DATA"

        try:
            df = pd.read_csv(path_csv)
            logging.info(f"Transformando {len(df)} registros para {ds_to_use}")

            # Formateo de fecha según requerimiento de carga
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
            
            # Limpieza del CSV crudo
            if os.path.exists(path_csv):
                os.remove(path_csv)
                
            return output_xlsx
        except Exception as e:
            logging.error(f"❌ Error en transform: {e}")
            raise

    @task
    def load(path_xlsx: str, ds=None):
        """Envía el Excel a Slack o notifica la ausencia de datos."""
        ds_to_use = FECHA_TEST if MODO_TEST else ds
        slack_hook = SlackHook(slack_conn_id="slack_conn")
        channel_id = "C0ARL1VRSVC" # Canal de Transvalores / Cashea

        if path_xlsx == "NO_DATA":
            msg = f"ℹ️ *Reporte Inceptia ({ds_to_use})*: No se registraron gestiones (Bot {BOT_ID_CASHEA})."
            slack_hook.get_conn().chat_postMessage(channel=channel_id, text=msg)
            return

        try:
            # Enviar archivo final a Slack
            slack_hook.get_conn().files_upload_v2(
                channel=channel_id,
                file=path_xlsx,
                title=f"Reporte Inceptia {ds_to_use}",
                initial_comment=f":bar_chart: *Gestiones automáticas BOT CASHEA* - Fecha: {ds_to_use}"
            )
            
            logging.info(f"✅ Reporte {ds_to_use} enviado exitosamente.")
            
            # Limpieza del Excel final
            if os.path.exists(path_xlsx):
                os.remove(path_xlsx)
        except Exception as e:
            logging.error(f"❌ Error al enviar a Slack: {e}")
            raise

    # --- Flujo de ejecución ---
    data_date = "{{ data_interval_start | ds }}"

    path_raw = extract(bot_id=BOT_ID_CASHEA, ds=data_date)
    path_final = transform(path_raw, ds=data_date)
    load(path_final, ds=data_date)

# Instanciación
dag_instance = inceptia_dag()