from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import pandas as pd
import os

# Importamos la clase desde tu estructura de carpetas
try:
    from include.inceptia_client import InceptiaAPI
except ImportError as e:
    logging.error(f"No se pudo encontrar include/inceptia_client.py: {e}")
    raise

# ==========================================================
# CONFIGURACIÓN DE PRUEBA
# ==========================================================
FECHA_PRUEBA = "2026-04-07"
BOT_ID = 806
ESTADO_TEST = "Transferir"

default_args = {
    "owner": "Julián",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="test_inceptia_diagnostico_completo",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,  # Solo ejecución manual
    catchup=False,
    default_args=default_args,
    tags=["test", "debug", "transvalores"],
)
def diagnostico_inceptia():

    @task
    def verificar_entorno():
        """Verifica que las variables de entorno estén presentes antes de llamar a la API."""
        vars_to_check = ["INCEPTIA_EMAIL", "INCEPTIA_PASSWORD", "INCEPTIA_BASE_URL"]
        missing = [v for v in vars_to_check if not os.getenv(v)]
        
        if missing:
            error_msg = f"❌ Faltan variables de entorno: {', '.join(missing)}"
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        logging.info("✅ Variables de entorno verificadas correctamente.")
        return True

    @task
    def test_universo_completo(env_ok):
        """Prueba extraer todos los casos sin filtros."""
        if not env_ok: return
        
        api = InceptiaAPI()
        logging.info(f"🚀 Iniciando TEST 1: Universo completo para {FECHA_PRUEBA}")
        
        df = api.obtener_todos_los_casos(BOT_ID, FECHA_PRUEBA, FECHA_PRUEBA, estado=None)
        
        if df is None:
            raise Exception("La API devolvió None (posible fallo de conexión o login)")

        if not df.empty:
            logging.info(f"✅ ÉXITO: Se encontraron {len(df)} registros.")
            # Verificamos si la columna esperada existe antes de imprimir
            col_estado = 'case_result.name'
            if col_estado in df.columns:
                logging.info(f"Estados detectados: {df[col_estado].unique().tolist()}")
            else:
                logging.warning(f"⚠️ La columna '{col_estado}' no está en el DataFrame. Columnas disponibles: {df.columns.tolist()}")
        else:
            logging.warning(f"⚠️ El DataFrame volvió vacío para {FECHA_PRUEBA}.")
        
        return len(df)

    @task
    def test_filtro_especifico(universo_count):
        """Prueba filtrar por el estado específico 'Transferir'."""
        api = InceptiaAPI()
        logging.info(f"🚀 Iniciando TEST 2: Filtrando por '{ESTADO_TEST}' para {FECHA_PRUEBA}")
        
        df = api.obtener_todos_los_casos(BOT_ID, FECHA_PRUEBA, FECHA_PRUEBA, estado=ESTADO_TEST)
        
        if df is None:
            raise Exception("La API devolvió None en el segundo test.")

        if not df.empty:
            logging.info(f"✅ ÉXITO: Se encontraron {len(df)} registros filtrados.")
            col_estado = 'case_result.name'
            if col_estado in df.columns:
                logging.info(f"Verificación de filtro: {df[col_estado].unique()}")
        else:
            logging.info(f"ℹ️ No se encontraron casos con estado '{ESTADO_TEST}' para este día específico.")
        
        return len(df)

    # Flujo de ejecución lineal
    env_status = verificar_entorno()
    count_all = test_universo_completo(env_status)
    test_filtro_especifico(count_all)

dag_instance = diagnostico_inceptia()