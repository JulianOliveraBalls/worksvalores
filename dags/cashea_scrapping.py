import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.slack.hooks.slack import SlackHook

PATH_PAGOS = "/tmp/reporte_pagos.csv"
PATH_ASIGNACIONES = "/tmp/reporte_asignaciones.csv"

with DAG(
    'cashea_retool_extractor',
    default_args={
        'retries': 1, 
        'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2026, 4, 12),
    schedule_interval='0 0,12,17,20 * * *',
    catchup=False
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
        from datetime import timedelta

        # 1. Ajuste de fecha local
        logical_date = kwargs['logical_date']
        fecha_local = (logical_date - timedelta(hours=3)).strftime('%Y-%m-%d')
        logging.info(f"🔎 Iniciando procesamiento para la fecha local: {fecha_local}")
        
        slack_hook = SlackHook(slack_conn_id="slack_conn")
        canal = "C0ARL1VRSVC"
        mensaje_sumatoria = ""
        
        # 2. Procesar Sumatoria de Pagos
        if os.path.exists(PATH_PAGOS):
            try:
                logging.info(f"📂 Leyendo archivo: {PATH_PAGOS}")
                df = pd.read_csv(PATH_PAGOS, low_memory=False)
                
                # LOG: Ver columnas y primeras filas
                logging.info(f"📊 Columnas detectadas: {df.columns.tolist()}")
                logging.info(f"📊 Muestra de datos:\n{df[['montoPagado', 'fechaEfectivaPago']].head(3)}")

                # Limpieza de Monto: Forzamos numérico y eliminamos posibles espacios
                df['montoPagado'] = df['montoPagado'].astype(str).str.replace(',', '.') # Por si viene con coma
                df['montoPagado'] = pd.to_numeric(df['montoPagado'], errors='coerce').fillna(0)
                
                # Limpieza de Fecha: Manejo de strings con milisegundos
                df['fecha_clean'] = pd.to_datetime(df['fechaEfectivaPago'], errors='coerce').dt.strftime('%Y-%m-%d')
                
                # LOG: Ver cuántas fechas coinciden
                conteo_fechas = df['fecha_clean'].value_counts().to_dict()
                logging.info(f"📅 Distribución de fechas en el CSV: {conteo_fechas}")

                # Filtrar y Sumar
                pagos_hoy = df[df['fecha_clean'] == fecha_local]
                logging.info(f"✅ Filas encontradas para hoy ({fecha_local}): {len(pagos_hoy)}")
                
                total_recaudado = pagos_hoy['montoPagado'].sum()
                mensaje_sumatoria = f"\n💰 *Total recaudado hoy ({fecha_local}):* ${total_recaudado:,.2f}"
                
            except Exception as e:
                logging.error(f"❌ Error detallado: {str(e)}", exc_info=True)
                mensaje_sumatoria = f"\n⚠️ Error al calcular sumatoria: {str(e)}"

        # 3. Verificar archivos
        archivos = [PATH_PAGOS, PATH_ASIGNACIONES]
        presentes = [f for f in archivos if os.path.exists(f)]

        if not presentes:
            slack_hook.get_conn().chat_postMessage(channel=canal, text=f"ℹ️ *Reporte ({fecha_local})*: Sin archivos.")
            return

        # 4. Enviar a Slack
        slack_hook.get_conn().chat_postMessage(
            channel=canal, 
            text=f":bar_chart: *Reporte Automático Transvalores* - Fecha: {fecha_local}{mensaje_sumatoria}"
        )

        for path in presentes:
            file_name = os.path.basename(path)
            slack_hook.get_conn().files_upload_v2(
                channel=canal,
                file=path,
                title=f"{file_name} - {fecha_local}"
            )
            os.remove(path)
            logging.info(f"🗑️ {file_name} eliminado.")

    # Definición del flujo
    tarea_extraccion >> procesar_y_enviar_slack()