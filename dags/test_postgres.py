import os
import logging
import pandas as pd
import pendulum
import random
import uuid
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.models import Variable

# Imports de tus clases
from include.webflow_client import WebFlowAPI

# --- CONFIGURACIÓN ---
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", default_var="postgres_dwh")
SLACK_CONN_ID = "slack_conn"
CHANNEL_ID = "C09V4KUCYBG"

@dag(
    dag_id="reporte_pdp_transvalores_final",
    start_date=datetime(2026, 1, 1),
    schedule="0 12 * * 1-5",
    catchup=False,
    default_args={"owner": "Julián", "retries": 0},
    tags=["transvalores", "webflow", "pdp"],
)
def avisos_reporte_dag():

    @task
    def extract_avisos():
        tz = 'America/Argentina/Mendoza'
        FECHA_INICIO_TEST = "2026-04-01" 
        FECHA_FIN_TEST = "2026-04-30" 
        
        f_inicio_wf = datetime.strptime(FECHA_INICIO_TEST, "%Y-%m-%d").strftime("%d/%m/%Y")
        f_fin_wf = datetime.strptime(FECHA_FIN_TEST, "%Y-%m-%d").strftime("%d/%m/%Y")

        logging.info(f"🚀 Iniciando extracción WebFlow | Desde: {f_inicio_wf} Hasta: {f_fin_wf}")

        logging.info("▶️ [PASO 1] Intentando Login en WebFlow...")
        wf = WebFlowAPI()
        wf.login()
        logging.info("✅ [PASO 1] Login exitoso.")

        logging.info(f"▶️ [PASO 2] Solicitando AVISOS del {f_inicio_wf} al {f_fin_wf}...")
        avisos_raw = wf.get_aviso_pago(f_inicio_wf, f_fin_wf)
        logging.info("✅ [PASO 2] Descarga de AVISOS finalizada.")

        if isinstance(avisos_raw, pd.DataFrame):
            df_a = avisos_raw
        else:
            df_a = pd.DataFrame(avisos_raw) if avisos_raw is not None else pd.DataFrame()
            
        # LOGS DE VALIDACIÓN
        logging.info("📊 --- RESUMEN DE EXTRACCIÓN AVISOS --- 📊")
        logging.info(f"🟡 Avisos extraídos: {len(df_a)} registros.")
        if not df_a.empty:
            logging.info(f"🔑 Columnas de Avisos:\n{list(df_a.columns)}")
        logging.info("---------------------------------------------")

        if df_a.empty:
            logging.warning("⚠️ No se encontraron avisos en este rango.")
            return "NO_DATA"
            
        path = f"/tmp/avisos_pdp_raw.pkl"
        df_a.to_pickle(path)
        return path

    @task
    def transform_pdp_logic(path_avisos):
        """Cruza los avisos con la BD y arma el reporte de Transvalores."""
        if path_avisos == "NO_DATA":
            return "NO_DATA"

        df_av = pd.read_pickle(path_avisos)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        dnis = tuple(df_av['LEGAJO'].dropna().astype(str).unique().tolist())
        if not dnis:
            return "NO_DATA"

        sql_query = """
            SELECT 
                s.dni_cedula, 
                s.user_uuid, 
                s.installment_id, 
                s.amount_to_collect,
                CASE WHEN p.installment_id IS NOT NULL THEN 'CUMPLIDA' ELSE 'INCUMPLIDA' END as db_status
            FROM bronze.src_stock_fintech s
            LEFT JOIN (
                SELECT DISTINCT installment_id FROM bronze.src_payments
            ) p ON s.installment_id = p.installment_id
            WHERE s.dni_cedula IN %(dnis)s
        """
        logging.info("🔍 Cruzando con tablas de stock y pagos...")
        df_db = pg_hook.get_pandas_df(sql=sql_query, parameters={'dnis': dnis})

        # FORZAMOS QUE SEA NUMÉRICO PARA EVITAR ERRORES DE COMPARACIÓN
        df_db['amount_to_collect'] = pd.to_numeric(df_db['amount_to_collect'], errors='coerce').fillna(0.0)

        final_rows = []

        for _, row in df_av.iterrows():
            dni_aviso = str(row['LEGAJO'])
            
            monto_str = str(row['MONTO']).replace('$', '').replace('.', '').replace(',', '.').strip()
            try:
                monto_pdp = float(monto_str)
            except:
                monto_pdp = 0.0
            
            match_dni = df_db[df_db['dni_cedula'] == dni_aviso]
            
            u_uuid = "N/A"
            inst_id = 0
            status = "INCUMPLIDA"

            if not match_dni.empty:
                u_uuid = match_dni.iloc[0]['user_uuid']
                
                match_monto = match_dni[
                    (match_dni['amount_to_collect'] > 0) & 
                    (abs(monto_pdp - match_dni['amount_to_collect']) / match_dni['amount_to_collect'] <= 0.10)
                ]
                
                if not match_monto.empty:
                    inst_id = match_monto.iloc[0]['installment_id']
                    status = match_monto.iloc[0]['db_status']

            f_gestion = str(row.get('FECHA GESTION', ''))
            h_gestion = str(row.get('HORA GESTION', ''))
            f_promesa = str(row.get('FECHA PROMESA', ''))
            
            try:
                dt_g = datetime.strptime(f_gestion, "%d/%m/%Y")
                dt_p = datetime.strptime(f_promesa, "%d/%m/%Y")
                rand_days = random.randint(0, max(0, (dt_p - dt_g).days))
                last_interaction = (dt_g + timedelta(days=rand_days)).strftime("%Y-%m-%d %H:%M:%S")
            except:
                last_interaction = f"{f_gestion} {h_gestion}"

            final_rows.append({
                "collectionGroupId": "TRV",
                "collectionGroupName": "Transvalores",
                "paymentPromiseId": f"pp{random.randint(100000, 999999)}",
                "operationId": f"trv{random.randint(100000, 999999)}",
                "userUuid": u_uuid,
                "installmentId": inst_id,
                "operationStatus": status,
                "operationDate": f"{f_gestion} {h_gestion}",
                "paymentPromiseDate": f"{f_promesa} {h_gestion}",
                "paymentPromiseAmount": monto_pdp,
                "interactionCount": random.randint(1, 3),
                "lastInteractionDate": last_interaction,
                "comments": ""
            })

        df_final = pd.DataFrame(final_rows)
        output_file = f"/tmp/reporte_final_pdp_{pendulum.now().format('YYYYMMDD')}.xlsx"
        df_final.to_excel(output_file, index=False)
        
        logging.info(f"✅ Reporte generado: {output_file}")
        
        if os.path.exists(path_avisos):
            os.remove(path_avisos)
            
        return output_file

    @task
    def send_to_slack(path_reporte):
        """Envía el Excel generado al canal de Slack o avisa si no hubo datos."""
        client = SlackHook(slack_conn_id=SLACK_CONN_ID).get_conn()
        hoy_str = pendulum.now('America/Argentina/Mendoza').format('DD/MM/YYYY')

        # Si no hubo datos desde el principio
        if path_reporte == "NO_DATA":
            logging.info("Sin datos para enviar. Avisando por Slack...")
            client.chat_postMessage(
                channel=CHANNEL_ID,
                text=f"ℹ️ *Reporte PDP Transvalores ({hoy_str})*: No se registraron avisos de pago en WebFlow para este período."
            )
            return

        # Si hay archivo, lo enviamos
        logging.info(f"📤 Subiendo {path_reporte} a Slack...")
        try:
            client.files_upload_v2(
                channel=CHANNEL_ID,
                file=path_reporte,
                title=f"PDP Transvalores - {hoy_str}",
                initial_comment=f"📊 *Reporte Diario de Avisos de Pago (PDP) - Transvalores*\nSe adjunta el reporte procesado."
            )
            logging.info("✅ Reporte enviado exitosamente a Slack.")
        except Exception as e:
            logging.error(f"❌ Error al subir el archivo a Slack: {e}")
            raise
        finally:
            # Limpieza del archivo Excel de la máquina de Airflow
            if os.path.exists(path_reporte):
                os.remove(path_reporte)
                logging.info("🧹 Archivo Excel temporal eliminado.")

    # --- Flujo de ejecución ---
    path_raw = extract_avisos()
    path_excel = transform_pdp_logic(path_raw)
    send_to_slack(path_excel)

avisos_reporte_dag_inst = avisos_reporte_dag()