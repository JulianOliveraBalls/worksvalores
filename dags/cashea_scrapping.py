from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    'cashea_retool_extractor',
    default_args={
        'retries': 1, 
        'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2026, 4, 12),
    schedule_interval='0 9 * * *', # Todos los días a las 9 AM
    catchup=False
) as dag:

    tarea_extraccion = BashOperator(
        task_id='extraer_csv_retool',
        bash_command="""
        # 1. Aseguramos PATH para el usuario airflow
        export PATH=$PATH:/home/airflow/.local/bin
        
        # 2. Definimos la ruta de los navegadores (por si la variable del .yml no cargó a tiempo)
        export PLAYWRIGHT_BROWSERS_PATH=/opt/airflow/include/playwright_browsers

        # 3. Instalamos Chromium SOLO si no existe en el volumen persistente
        if [ ! -d "$PLAYWRIGHT_BROWSERS_PATH" ] || [ -z "$(ls -A $PLAYWRIGHT_BROWSERS_PATH)" ]; then
            echo "Instalando Chromium en volumen persistente..."
            python3 -m playwright install chromium
        fi
        
        # 4. Ejecutamos el extractor
        python3 /opt/airflow/include/extractor.py
        """
    )