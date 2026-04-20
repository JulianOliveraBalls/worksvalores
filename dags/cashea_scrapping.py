import os
import logging
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

PATH_PAGOS = "/opt/airflow/shared/reporte_pagos.csv"
PATH_ASIGNACIONES = "/opt/airflow/shared/reporte_asignaciones.csv"
BATCH_SIZE = 5000

POSTGRES_CONN_ID = Variable.get("postgres_conn_id", default_var="postgres_dwh")
SLACK_CONN_ID    = Variable.get("slack_conn_id",    default_var="slack_conn")
ID_CANAL         = Variable.get("cashea_slack_canal",       default_var="C09V4KUCYBG")


def alerta_slack_error(context):
    try:
        slack_hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
        dag_id    = context['dag'].dag_id
        task_id   = context['task_instance'].task_id
        exec_date = context['execution_date']
        slack_hook.get_conn().chat_postMessage(
            channel=ID_CANAL,
            text=(
                f":red_circle: *ERROR en DAG `{dag_id}`*\n"
                f"📌 Tarea: `{task_id}`\n"
                f"🕐 Fecha: {exec_date}\n"
                f"🔗 Revisá los logs en Airflow"
            )
        )
    except Exception as e:
        logging.error(f"❌ No se pudo enviar alerta a Slack: {e}")


default_args = {
    'owner': 'Julián',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alerta_slack_error
}


def parsear_fecha(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    val = str(val).strip()
    formatos = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y',
        '%Y-%m-%d',
    ]
    for fmt in formatos:
        try:
            return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    logging.warning(f"⚠️ Fecha no parseada: {val}")
    return None


def parsear_hora(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return pd.to_datetime(str(val).strip(), errors='coerce').strftime('%H:%M:%S')
    except Exception:
        return None


with DAG(
    'cashea_retool_extractor',
    default_args=default_args,
    start_date=datetime(2026, 4, 12),
    schedule_interval='5 0,12,17,21 * * *',
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
    def upsert_asignaciones(**kwargs):
        if not os.path.exists(PATH_ASIGNACIONES):
            logging.warning("⚠️ No existe archivo de asignaciones, saltando.")
            return

        tz_mza = pendulum.timezone("America/Argentina/Buenos_Aires")
        stock_date = kwargs['logical_date'].in_timezone(tz_mza).format('YYYY-MM-DD')

        df = pd.read_csv(PATH_ASIGNACIONES, low_memory=False, dtype=str)
        df = df.rename(columns={
            'ID cuota':                'installment_id',
            'Fecha Pago':              'payment_date',
            'Monto':                   'amount',
            'fee':                     'fee_amount',
            '#Cuota':                  'installment_number',
            'Monto por cobrar actual': 'amount_to_collect',
            'Capital asignada':        'capital_assigned',
            'ID orden':                'order_id',
            'Email usuario':           'user_email',
            'UUID usuario':            'user_uuid',
            'Telefono':                'phone',
            '#Cedula':                 'dni_cedula',
            'Nombre usuario':          'user_name',
            'Tramo inicial':           'initial_bracket',
            'Tramo actual':            'current_bracket',
            'Fecha asignacion cuota':  'due_date',
        })

        df['phone'] = df['phone'].astype(str).str.replace('+', '', regex=False)
        df['stock_date'] = stock_date

        cols = [
            'installment_id', 'payment_date', 'amount', 'fee_amount',
            'installment_number', 'amount_to_collect', 'capital_assigned',
            'order_id', 'user_email', 'user_uuid', 'phone', 'dni_cedula',
            'user_name', 'initial_bracket', 'current_bracket', 'due_date',
            'stock_date', 'portfolio'
        ]

        df = df[cols[:-1]].where(pd.notna(df[cols[:-1]]), None)
        df['portfolio'] = 'Cashea'

        update_cols = ['current_bracket', 'due_date', 'stock_date', 'amount_to_collect', 'phone', 'user_email', 'user_name']
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        sql = f"""
            INSERT INTO bronze.src_stock_fintech ({', '.join(cols)})
            VALUES %s
            ON CONFLICT (installment_id)
            DO UPDATE SET {update_set}, ingested_at = CURRENT_TIMESTAMP
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        total = 0

        try:
            with conn:
                with conn.cursor() as cur:
                    for i in range(0, len(df), BATCH_SIZE):
                        batch = df.iloc[i:i+BATCH_SIZE]
                        rows = [tuple(r) for r in batch.itertuples(index=False)]
                        execute_values(cur, sql, rows)
                        total += len(rows)
                        logging.info(f"✅ Asignaciones batch {i//BATCH_SIZE + 1}: {len(rows)} filas")
        finally:
            conn.close()
            os.remove(PATH_ASIGNACIONES)
            logging.info(f"🗑️ Asignaciones eliminadas. Total: {total} filas.")

    @task
    def upsert_pagos(**kwargs):
        if not os.path.exists(PATH_PAGOS):
            logging.warning("⚠️ No existe archivo de pagos, saltando.")
            return

        df = pd.read_csv(PATH_PAGOS, low_memory=False, dtype=str)
        df = df.rename(columns={
            'id_usuario':        'user_uuid',
            'id_cuota':          'installment_id',
            'nombreUsuario':     'user_name',
            'cedula':            'dni',
            'fechaAsignacion':   'assignment_date',
            'cuotaNumero':       'installment_number',
            'montoPagado':       'amount_paid',
            'fechaEfectivaPago': 'payment_datetime',
            'fechaCorte':        'cutoff_date',
        })

        df['payment_date'] = df['payment_datetime'].apply(parsear_fecha)
        df['payment_time'] = df['payment_datetime'].apply(parsear_hora)

        for col in ['cutoff_date', 'assignment_date']:
            df[col] = df[col].apply(parsear_fecha)

        cols = [
            'user_uuid', 'installment_id', 'user_name', 'dni',
            'assignment_date', 'installment_number', 'amount_paid',
            'payment_date', 'payment_time', 'cutoff_date', 'portfolio'
        ]

        df = df[cols[:-1]].where(pd.notna(df[cols[:-1]]), None)
        df['portfolio'] = 'Cashea'

        sql = f"""
            INSERT INTO bronze.src_payments ({', '.join(cols)})
            VALUES %s
            ON CONFLICT (installment_id, payment_date, payment_time)
            DO UPDATE SET
                cutoff_date = EXCLUDED.cutoff_date,
                ingested_at = CURRENT_TIMESTAMP
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        total = 0

        try:
            with conn:
                with conn.cursor() as cur:
                    for i in range(0, len(df), BATCH_SIZE):
                        batch = df.iloc[i:i+BATCH_SIZE]
                        rows = [tuple(r) for r in batch.itertuples(index=False)]
                        execute_values(cur, sql, rows)
                        total += len(rows)
                        logging.info(f"✅ Pagos batch {i//BATCH_SIZE + 1}: {len(rows)} filas")
        finally:
            conn.close()
            logging.info(f"Total pagos procesados: {total} filas.")

    @task
    def enviar_slack(**kwargs):
        tz_mza = pendulum.timezone("America/Argentina/Buenos_Aires")
        now_mza = pendulum.now(tz_mza)
        fecha_hoy = now_mza.format('YYYY-MM-DD')
        fecha_ayer = now_mza.subtract(days=1).format('YYYY-MM-DD')
        hora = now_mza.hour
        es_reporte_manana = (hora == 9)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        def consultar(fecha):
            sql = """
                SELECT
                    COALESCE(SUM(amount_paid::NUMERIC), 0) AS total,
                    COUNT(*) AS cantidad
                FROM bronze.src_payments
                WHERE payment_date = %s
                  AND portfolio = 'Cashea'
            """
            result = hook.get_first(sql, parameters=(fecha,))
            return float(result[0]), int(result[1])

        slack_hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
        total_hoy, count_hoy = consultar(fecha_hoy)
        total_ayer, count_ayer = consultar(fecha_ayer)

        if es_reporte_manana:
            slack_hook.get_conn().chat_postMessage(
                channel=ID_CANAL,
                text=(
                    f":night_with_stars: *CIERRE DEL DÍA ({fecha_ayer})*\n"
                    f"Total recaudado: ${total_ayer:,.2f}\n"
                    f"Cantidad de pagos: {count_ayer}"
                )
            )
            logging.info("✅ Enviado cierre del día anterior")

        slack_hook.get_conn().chat_postMessage(
            channel=ID_CANAL,
            text=(
                f":sunrise: *REPORTE DEL DÍA ({fecha_hoy})*\n"
                f"Total acumulado: ${total_hoy:,.2f}\n"
                f"Cantidad de pagos: {count_hoy}"
            )
        )
        logging.info("✅ Enviado reporte del día actual")

        if os.path.exists(PATH_PAGOS):
            os.remove(PATH_PAGOS)
            logging.info("🗑️ pagos.csv eliminado")

    tarea_extraccion >> [upsert_asignaciones(), upsert_pagos()] >> enviar_slack()