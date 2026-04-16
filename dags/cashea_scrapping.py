import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.slack.hooks.slack import SlackHook

# 1. RUTAS DE ARCHIVOS
PATH_PAGOS = "/tmp/reporte_pagos.csv"
PATH_ASIGNACIONES = "/tmp/reporte_asignaciones.csv"

default_args = {
    'owner': 'Julián',
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'cashea_retool_extractor',
    default_args=default_args,
    start_date=datetime(2026, 4, 12),
    # 00:00, 12:00, 15:00, 17:00, 20:00 Schedule
    # 21:00, 09:00, 12:00, 14:00, 17:00 Mendoza
    schedule_interval='0 0,12,15,17,20 * * *',
    catchup=False,
    tags=['cashea', 'prod']
) as dag:

    tarea_extraccion = BashOperator(
        task_id='extraer_csv_retool',
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        export PLAYWRIGHT_BROWSERS_PATH=/opt/airflow/include/playwright_browsers

        if [ ! -d "$PLAYWRIGHT_BROWSERS_PATH" ] || [ -z "$(ls -A $PLAYWRIGHT_BROWSERS_PATH)" ]; then
            python3 -m playwright install chromium
        fi
        
        python3 /opt/airflow/include/extractor.py
        """
    )

    @task
    def procesar_y_enviar_slack(**kwargs):
        from airflow.providers.slack.hooks.slack import SlackHook
        import pandas as pd
        import os
        import logging
        import pendulum


        tz_mza = pendulum.timezone("America/Argentina/Buenos_Aires")
        fecha_local = pendulum.now(tz_mza).format('YYYY-MM-DD')
        
        logging.info(f"🔎 Ejecutando reporte con fecha real de Mendoza: {fecha_local}")
        
        slack_hook = SlackHook(slack_conn_id="slack_conn")
        ID_CANAL = "C0ARL1VRSVC"
        mensaje_sumatoria = ""
        
        # 2. Procesar Sumatoria de Pagos
        if os.path.exists(PATH_PAGOS):
            try:
                df = pd.read_csv(PATH_PAGOS, low_memory=False)
                df['montoPagado'] = pd.to_numeric(df['montoPagado'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                df['fecha_clean'] = pd.to_datetime(df['fechaEfectivaPago'], errors='coerce').dt.strftime('%Y-%m-%d')
                
                # Filtramos por la fecha de hoy en Mendoza
                pagos_hoy = df[df['fecha_clean'] == fecha_local]
                total_recaudado = pagos_hoy['montoPagado'].sum()
                
                mensaje_sumatoria = f"\n💰 *Total recaudado hoy ({fecha_local}):* ${total_recaudado:,.2f}"
                logging.info(f"✅ Filas encontradas para {fecha_local}: {len(pagos_hoy)} | Total: {total_recaudado}")
                
            except Exception as e:
                logging.error(f"❌ Error en sumatoria: {str(e)}")
                mensaje_sumatoria = f"\n⚠️ Error al calcular sumatoria."

        # 3. Verificar archivos y enviar a Slack
        archivos = [PATH_PAGOS, PATH_ASIGNACIONES]
        presentes = [f for f in archivos if os.path.exists(f)]

        if presentes:
            slack_hook.get_conn().chat_postMessage(
                channel=ID_CANAL, 
                text=f":bar_chart: *Reporte Automático Transvalores* - Fecha: {fecha_local}{mensaje_sumatoria}"
            )

            for path in presentes:
                slack_hook.get_conn().files_upload_v2(
                    channel=ID_CANAL,
                    file=path,
                    title=f"{os.path.basename(path)} - {fecha_local}"
                )
                os.remove(path)
                logging.info(f"🗑️ {path} enviado y eliminado.")
        else:
            logging.error(f"❌ No se encontraron archivos para enviar.")
    tarea_extraccion >> procesar_y_enviar_slack()