from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# IMPORTANTE: Ajustar el path según tu estructura de carpetas
# Si está en /include, podrías necesitar agregar el path al sys.path o usar:
from webflow_client import WebFlowAPI 

def run_test(method_name, **kwargs):
    # Instanciamos la clase del include
    api = WebFlowAPI()
    
    if not api.login():
        raise Exception("❌ Fallo crítico en Login. Revisar credenciales.")

    # Rango de fechas: desde el 7 de abril hasta hoy
    f_inicio = "07/04/2026"
    f_fin = datetime.now().strftime("%d/%m/%Y")
    
    logging.info(f"🧪 Iniciando TEST para: {method_name}")
    logging.info(f"📅 Rango: {f_inicio} a {f_fin}")

    # Selección dinámica del método
    if method_name == "gestiones": df = api.get_gestiones(f_inicio, f_fin)
    elif method_name == "llamados": df = api.get_llamados(f_inicio, f_fin)
    elif method_name == "perdidas": df = api.get_perdidas(f_inicio, f_fin)
    elif method_name == "aviso_pago": df = api.get_aviso_pago(f_inicio, f_fin)
    elif method_name == "entregas": df = api.get_entregas(f_inicio, f_fin)

    if not df.empty:
        logging.info(f"✅ ÉXITO: {method_name} devolvió {len(df)} registros.")
        logging.info(f"📌 Columnas: {df.columns.tolist()}")
    else:
        logging.error(f"❌ El reporte {method_name} no trajo datos (o devolvió HTML).")

# --- DEFINICIÓN DEL DAG ---

default_args = {
    'owner': 'julian.olivera',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 0
}

with DAG(
    'test_webflow_v4_separated',
    default_args=default_args,
    description='DAG de prueba con clase separada en include',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'julian']
) as dag:

    reportes = ['gestiones', 'llamados', 'perdidas', 'aviso_pago', 'entregas']
    
    for rep in reportes:
        PythonOperator(
            task_id=f'test_{rep}',
            python_callable=run_test,
            op_kwargs={'method_name': rep}
        )