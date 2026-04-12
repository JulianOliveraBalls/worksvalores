from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging
import pandas as pd

# --- SOLUCIÓN DE RUTAS ---
# Esto fuerza a Python a encontrar la carpeta 'include' sin importar desde dónde se ejecute
dag_file_path = os.path.dirname(os.path.abspath(__file__))
if dag_file_path not in sys.path:
    sys.path.append(dag_file_path)

# Intentamos el import con manejo de errores para que Airflow nos diga qué pasa
from include.webflow_client import WebFlowAPI

def extract_webflow_data(**kwargs):
    # Rango de test solicitado: 09/04 al 10/04
    f_inicio = "09/04/2026"
    f_fin = "10/04/2026"
    
    logging.info(f"🚀 Iniciando extracción masiva WebFlow | Rango: {f_inicio} al {f_fin}")

    # Credenciales de Julián
    api = WebFlowAPI("julian.olivera", "Julian2016")
    if not api.login():
        raise Exception("❌ Error crítico: Login fallido en WebFlow.")

    dataframes = {}

    # --- EJECUCIÓN DE TODOS LOS REPORTES ---
    dataframes["GESTIONES"] = api.get_gestiones(f_inicio, f_fin, id_usuario=0, rel_usuario="=")
    dataframes["AVISOS_PAGO"] = api.get_aviso_pago(f_inicio, f_fin)
    dataframes["ENTREGAS"] = api.get_entregas(ids_cliente=[330, 260], estados=[302, 303])
    dataframes["LLAMADOS"] = api.get_llamados(f_inicio, f_fin)
    dataframes["PERDIDAS"] = api.get_perdidas(f_inicio, f_fin)
    dataframes["CONVENIOS"] = api.get_convenios_pago(f_inicio, f_fin)

    logging.info("\n" + "="*50 + "\nRESUMEN DE DATOS OBTENIDOS\n" + "="*50)
    
    for nombre, df in dataframes.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            logging.info(f"✅ {nombre}: {len(df)} registros.")
            logging.info(f"📋 Columnas: {df.columns.tolist()}")
            logging.info(f"📄 Muestra (head):\n{df.head(5).to_string()}\n")
        else:
            logging.warning(f"⚠️ {nombre}: No se obtuvieron registros.")

    logging.info("="*50 + "\n✅ Proceso terminado con éxito.")

# --- DEFINICIÓN DEL DAG ---
default_args = {
    'owner': 'Julián',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'test_webflow_full_reports_v2', # Le cambié el nombre para forzar el refresh en la UI
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=['test', 'transvalores', 'debug']
) as dag:

    task_extract = PythonOperator(
        task_id='extract_and_audit_webflow',
        python_callable=extract_webflow_data
    )