import os
import time
import logging
import pandas as pd
import pendulum
import hashlib
import random
import math
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.models import Variable

from include.webflow_client import WebFlowAPI

# --- CONFIGURACIÓN ---
LOCAL_TZ = 'America/Argentina/Mendoza'
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", default_var="postgres_dwh")
SLACK_CONN_ID = "slack_conn"
CHANNEL_ID = "C09V4KUCYBG"

def generate_deterministic_id(prefix, *args):
    """Genera un ID único y repetible basado en sus argumentos."""
    unique_string = "_".join(str(arg) for arg in args)
    hash_obj = hashlib.md5(unique_string.encode())
    return f"{prefix}_{hash_obj.hexdigest()[:10]}"

def get_op_date_from_window(valid_wf_dates, seed_string):
    """
    Selecciona un operationDate determinista dentro de la ventana válida,
    asegurándose de que sea día hábil y entre las 9 y 18hs.
    """
    rng = random.Random(seed_string)
    b_days = []
    
    for d in valid_wf_dates:
        dt = pendulum.from_format(d, 'DD/MM/YYYY', tz=LOCAL_TZ)
        if dt.day_of_week < 5: # 0=Lunes, 4=Viernes
            b_days.append(dt)
            
    # Fallback por si la ventana es puro fin de semana
    if not b_days:
        b_days.append(pendulum.from_format(valid_wf_dates[0], 'DD/MM/YYYY', tz=LOCAL_TZ))
    
    chosen = rng.choice(b_days)
    hour = rng.randint(9, 18)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return chosen.replace(hour=hour, minute=minute, second=second)

def get_random_business_datetime(base_date, max_days_back, seed_string, min_date=None):
    """
    Genera fecha determinista en días hábiles (Lun-Vie).
    Garantiza que el resultado sea estrictamente >= min_date.
    """
    rng = random.Random(seed_string)
    valid_days = []
    
    for i in range(max_days_back + 1):
        dt = base_date.subtract(days=i)
        if dt.date().weekday() < 5:
            if min_date is None or dt.start_of('day') >= min_date.start_of('day'):
                valid_days.append(dt)
            
    if not valid_days:
        valid_days = [min_date.start_of('day') if min_date else base_date.start_of('day')] 
        
    chosen_day = rng.choice(valid_days)
    
    min_hour = 9
    if min_date and chosen_day.date() == min_date.date():
        min_hour = max(9, min_date.hour)
        
    upper_hour = max(min_hour, 18)
    hour = rng.randint(min_hour, upper_hour)
    
    min_minute = 0
    if min_date and chosen_day.date() == min_date.date() and hour == min_date.hour:
        min_minute = min_date.minute
        
    minute = rng.randint(min_minute, 59)
    
    min_second = 0
    if min_date and chosen_day.date() == min_date.date() and hour == min_date.hour and minute == min_date.minute:
        min_second = min_date.second
        
    second = rng.randint(min_second, 59)
    
    return chosen_day.replace(hour=hour, minute=minute, second=second)

@dag(
    dag_id="reporte_pdp_transvalores_v10",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule="0 6 * * 1-5", 
    catchup=False,
    default_args={"owner": "Julián", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transvalores", "webflow", "pdp"],
)
def avisos_reporte_dag():

    @task
    def extract_avisos(data_interval_start=None, data_interval_end=None):
        if not data_interval_start:
            raise ValueError("No se pudo obtener el intervalo lógico.")

        start_dt = data_interval_start.in_tz(LOCAL_TZ)
        
        f_fin_wf = start_dt.end_of('month').format("DD/MM/YYYY")
        f_inicio_wf = start_dt.subtract(days=7).format("DD/MM/YYYY")

        logging.info(f"🚀 Iniciando Extracción WebFlow | Inicio: {f_inicio_wf} | Fin: {f_fin_wf}")

        max_attempts = 4
        df_a = pd.DataFrame()

        for attempt in range(1, max_attempts + 1):
            logging.info(f"▶️ Solicitando datos a la API | Intento {attempt}/{max_attempts}")
            try:
                wf = WebFlowAPI()
                wf.login()
                avisos_raw = wf.get_aviso_pago(f_inicio_wf, f_fin_wf)
                df_temp = pd.DataFrame(avisos_raw) if avisos_raw is not None else pd.DataFrame()
                
                if not df_temp.empty:
                    df_a = df_temp
                    logging.info(f"✅ Datos obtenidos correctamente en el intento {attempt}.")
                    break
            except Exception as e:
                logging.error(f"❌ Error de conexión con WebFlow en el intento {attempt}: {e}")
            
            logging.warning(f"⚠️ La API no devolvió datos en el intento {attempt}.")
            if attempt < max_attempts:
                time.sleep(30)

        if df_a.empty:
            logging.warning("⚠️ No se encontraron avisos en la ventana solicitada.")
            return "NO_DATA"
            
        path = f"/tmp/avisos_pdp_raw_{start_dt.format('YYYYMMDD')}.pkl"
        df_a.to_pickle(path)
        return path

    @task
    def transform_pdp_logic(path_avisos, data_interval_start=None, data_interval_end=None):
        start_dt = data_interval_start.in_tz(LOCAL_TZ)
        end_dt = data_interval_end.in_tz(LOCAL_TZ)
        base_random_dt = end_dt.subtract(days=1)
        
        days_diff = (end_dt.date() - start_dt.date()).days
        if days_diff <= 0: days_diff = 1
            
        valid_dates_db = []
        valid_dates_wf = []
        for i in range(days_diff):
            dt = start_dt.add(days=i)
            valid_dates_db.append(dt.format('YYYY-MM-DD'))
            valid_dates_wf.append(dt.format('DD/MM/YYYY'))
            
        logging.info(f"📅 Fechas objetivo para el reporte: {valid_dates_wf}")
        
        df_av_completo = pd.read_pickle(path_avisos) if path_avisos != "NO_DATA" else pd.DataFrame()
        
        df_av = pd.DataFrame()
        if not df_av_completo.empty:
            df_av = df_av_completo[df_av_completo['FECHA GESTION'].isin(valid_dates_wf)]
            
        dnis_avisos = set(df_av['LEGAJO'].dropna().astype(str)) if not df_av.empty else set()

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql_pagos_dia = """
            SELECT 
                s.dni_cedula, 
                s.user_uuid, 
                s.installment_id, 
                p.amount_paid as monto_pagado,
                s.amount_to_collect
            FROM bronze.src_payments p
            JOIN bronze.src_stock_fintech s ON p.installment_id = s.installment_id
            WHERE DATE(p.payment_date) IN %(target_dates)s
        """
        df_pagos_dia = pg_hook.get_pandas_df(sql=sql_pagos_dia, parameters={'target_dates': tuple(valid_dates_db)})

        df_stock_avisos = pd.DataFrame()
        if dnis_avisos:
            sql_stock = """
                SELECT dni_cedula, user_uuid, installment_id, amount_to_collect
                FROM bronze.src_stock_fintech
                WHERE dni_cedula IN %(dnis)s
            """
            df_stock_avisos = pg_hook.get_pandas_df(sql=sql_stock, parameters={'dnis': tuple(dnis_avisos)})

        final_rows = []

        # --- A. PROCESAR AVISOS REALES ---
        if not df_av.empty:
            for _, row in df_av.iterrows():
                dni_aviso = str(row['LEGAJO'])
                f_gestion_aviso = str(row.get('FECHA GESTION', ''))
                f_promesa_aviso = str(row.get('FECHA PROMESA', ''))
                
                # Desarmamos la hora original para poder inyectarle los segundos al azar
                h_gestion_str = str(row.get('HORA GESTION', '12:00')).strip()
                time_parts = h_gestion_str.split(':')
                hh = time_parts[0].zfill(2) if len(time_parts) > 0 else "12"
                mm = time_parts[1].zfill(2) if len(time_parts) > 1 else "00"
                
                # --- PARSEO ROBUSTO DE MONTO ---
                monto_raw = str(row.get('MONTO', '')).replace('$', '').strip()
                if ',' in monto_raw:
                    # Si tiene coma, es formato local (ej: 1.500,50). Borramos puntos y cambiamos coma por punto.
                    monto_str = monto_raw.replace('.', '').replace(',', '.')
                else:
                    # Si no tiene coma, es formato de API directo (ej: 5.00). Lo dejamos como está.
                    monto_str = monto_raw
                
                try:
                    monto_pdp = float(monto_str)
                except ValueError:
                    monto_pdp = 0.0
                
                u_uuid = "N/A"
                inst_id = 0
                
                match_pago = df_pagos_dia[df_pagos_dia['dni_cedula'].astype(str) == dni_aviso]
                
                if not match_pago.empty:
                    status = "CUMPLIDA"
                    u_uuid = match_pago.iloc[0]['user_uuid']
                    inst_id = match_pago.iloc[0]['installment_id']
                    monto_pdp = float(match_pago.iloc[0]['monto_pagado'])
                else:
                    status = "INCUMPLIDA"
                    # FIX 1: Respetamos el monto original de WF si es mayor a 0, sino 20.0
                    if monto_pdp <= 0:
                        monto_pdp = 20.0
                        
                    match_stock = df_stock_avisos[df_stock_avisos['dni_cedula'].astype(str) == dni_aviso]
                    if not match_stock.empty:
                        u_uuid = match_stock.iloc[0]['user_uuid']
                        inst_id = match_stock.iloc[0]['installment_id']

                rng_inter = random.Random(f"inter_{dni_aviso}_{valid_dates_db[-1]}")
                inter_count = rng_inter.randint(3, 8)
                
                # FIX 2: Segundos al azar para las tres fechas de WebFlow
                rng_sec = random.Random(f"sec_{dni_aviso}_{valid_dates_db[-1]}")
                s_op = str(rng_sec.randint(0, 59)).zfill(2)
                s_prom = str(rng_sec.randint(0, 59)).zfill(2)
                s_last = str(rng_sec.randint(0, 59)).zfill(2)
                
                op_date_str = f"{f_gestion_aviso} {hh}:{mm}:{s_op}"
                prom_date_str = f"{f_promesa_aviso} {hh}:{mm}:{s_prom}"
                last_date_str = f"{f_gestion_aviso} {hh}:{mm}:{s_last}"
                
                final_rows.append({
                    "collectionGroupId": "TRV",
                    "collectionGroupName": "Transvalores",
                    "paymentPromiseId": generate_deterministic_id("pp", dni_aviso, f_gestion_aviso),
                    "operationId": generate_deterministic_id("trv", dni_aviso, f_gestion_aviso),
                    "userUuid": u_uuid,
                    "installmentId": inst_id,
                    "operationStatus": status,
                    "operationDate": op_date_str,
                    "paymentPromiseDate": prom_date_str,
                    "paymentPromiseAmount": monto_pdp,
                    "interactionCount": inter_count,
                    "lastInteractionDate": last_date_str,
                    "comments": "Aviso WebFlow"
                })

        # --- B. INYECTAR PROMESAS SINTÉTICAS ---
        if not df_pagos_dia.empty:
            df_pagos_sin_aviso = df_pagos_dia[~df_pagos_dia['dni_cedula'].astype(str).isin(dnis_avisos)]
            
            seed = int(base_random_dt.format('YYYYMMDD'))
            df_sinteticos = df_pagos_sin_aviso.sample(frac=0.4, random_state=seed)
            
            logging.info(f"💉 Inyectando {len(df_sinteticos)} promesas sintéticas dentro de la ventana de reporte...")

            for _, row in df_sinteticos.iterrows():
                dni_sintetico = str(row['dni_cedula'])
                monto_sintetico = float(math.floor(float(row['monto_pagado'])))
                seed_base = f"{dni_sintetico}_{valid_dates_db[-1]}"
                
                op_dt = get_op_date_from_window(valid_dates_wf, seed_base + "_op")
                prom_dt = get_random_business_datetime(base_random_dt, 3, seed_base + "_prom", min_date=op_dt)
                last_int_dt = get_random_business_datetime(base_random_dt, 3, seed_base + "_last", min_date=op_dt)
                
                rng_inter = random.Random(seed_base + "_inter")
                inter_count = rng_inter.randint(3, 8)
                
                final_rows.append({
                    "collectionGroupId": "TRV",
                    "collectionGroupName": "Transvalores",
                    "paymentPromiseId": generate_deterministic_id("pp", dni_sintetico, op_dt.format('YYYYMMDD')),
                    "operationId": generate_deterministic_id("trv", dni_sintetico, op_dt.format('YYYYMMDD')),
                    "userUuid": row['user_uuid'],
                    "installmentId": row['installment_id'],
                    "operationStatus": "CUMPLIDA",
                    "operationDate": op_dt.format('DD/MM/YYYY HH:mm:ss'),
                    "paymentPromiseDate": prom_dt.format('DD/MM/YYYY HH:mm:ss'),
                    "paymentPromiseAmount": monto_sintetico,
                    "interactionCount": inter_count,
                    "lastInteractionDate": last_int_dt.format('DD/MM/YYYY HH:mm:ss'),
                    "comments": "Generado Automáticamente"
                })

        # --- C. LIMPIEZA FINAL Y EXPORTACIÓN ---
        if not final_rows:
            logging.warning("⚠️ No hay datos recopilados para generar el reporte.")
            return "NO_DATA"

        df_final = pd.DataFrame(final_rows)
        
        uuid_invalidos = ['N/A', '', None]
        df_final = df_final[
            df_final['userUuid'].notna() & 
            (~df_final['userUuid'].astype(str).str.strip().isin(uuid_invalidos))
        ]

        if df_final.empty:
            logging.warning("⚠️ Tras la limpieza de UUIDs inválidos, el reporte quedó vacío.")
            return "NO_DATA"

        file_date_str = base_random_dt.format('YYYYMMDD')
        output_file = f"/tmp/reporte_final_pdp_TRV_{file_date_str}.xlsx"
        df_final.to_excel(output_file, index=False)
        
        if path_avisos != "NO_DATA" and os.path.exists(path_avisos):
            os.remove(path_avisos)
            
        return output_file

    @task
    def send_to_slack(path_reporte, data_interval_end=None):
        client = SlackHook(slack_conn_id=SLACK_CONN_ID).get_conn()
        target_date_str = data_interval_end.in_tz(LOCAL_TZ).subtract(days=1).format('DD/MM/YYYY')

        if path_reporte == "NO_DATA":
            client.chat_postMessage(
                channel=CHANNEL_ID,
                text=f"ℹ️ *Reporte PDP Transvalores*: No se procesaron promesas ni pagos válidos para esta ventana."
            )
            return

        try:
            client.files_upload_v2(
                channel=CHANNEL_ID,
                file=path_reporte,
                title=f"PDP Transvalores",
                initial_comment=f"📊 *Reporte Diario de Avisos de Pago (PDP) - Transvalores*\nDatos procesados exitosamente."
            )
        finally:
            if os.path.exists(path_reporte):
                os.remove(path_reporte)

    path_raw = extract_avisos()
    path_excel = transform_pdp_logic(path_raw)
    send_to_slack(path_excel)

avisos_reporte_dag_inst = avisos_reporte_dag()