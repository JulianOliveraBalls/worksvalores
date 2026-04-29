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
SLACK_CONN_ID    = Variable.get("slack_conn_id",    default_var="slack_conn")
CHANNEL_ID        = Variable.get("cashea_slack_canal",       default_var="C09V4KUCYBG")

MAPA_EVENTOS = {
    "No Contactado": ("Sin Contacto", "No Contesta"),
    "Mensaje en el Contestador": ("Sin Contacto", "Buzon de voz"),
    "Contactado": ("Contacto Titular", "Volver a Llamar"),
    "Promesa de Pago": ("Contacto Titular", "Compromiso de Pago Total"),
    "Tel Erroneo": ("Sin Contacto", "Equivocado - No Corresponde"),
    "Negociacion": ("Contacto Titular", "Seguimiento-Interesado Negociar"),
    "Aviso de Pago": ("Contacto Titular", "Recordatorio de Pago Vigente"),
    "Cliente informa cancelación": ("Contacto Titular", "YA PAGO - NO SUBIO EL COMPROBANTE"),
    "Contactado - Sin trabajo": ("Contacto Titular", "TT Sin Trabajo/Enfermedad/Falta Liquidez"),
    "Fallecido": ("Contacto Tercero", "TT Fallecido"),
    "Recordatorio Hecho": ("Contacto Titular", "Recordatorio de Pago Vigente"),
    "Contactado - Cobro irregular del sueldo": ("Contacto Titular", "Desacuerdo con Monto Adeudado"),
    "Entrante": ("Contacto Titular", "Volver a Llamar"),
    "No Reconoce Deuda": ("Contacto Titular", "Desconoce Deuda/Posible Fraude"),
}

OPERADORES_LISTA = [
    "MARIA CAIRAT", "YANINA COLLADO", "Valeria Cerda", "ANDREA CARRAI",
    "TAMARA MONTECINOS", "DANIELA MELA", "MARIA PAZ LUCERO", "AGUSTIN BIZZOTTO",
    "Sabrina Cordoba", "Maria Panella", "YESICA ALBORNOZ", "ROCIO ESCRIBANO",
    "ALMA VILLEGAS", "PAULA ZARATE"
]

# --- FUNCIONES AUXILIARES ---

def format_to_iso(date_str):
    """Convierte una fecha DD/MM/YYYY a YYYY-MM-DD."""
    if not date_str or pd.isna(date_str): return "1970-01-01"
    try:
        return pendulum.from_format(str(date_str).strip(), 'DD/MM/YYYY').format('YYYY-MM-DD')
    except:
        return str(date_str).strip()

def generate_deterministic_id(prefix, *args):
    unique_string = "_".join(str(arg) for arg in args)
    hash_obj = hashlib.md5(unique_string.encode())
    return f"{prefix}_{hash_obj.hexdigest()[:10]}"

def parse_operator_user(full_name):
    """Ejemplo: MARIA CAIRAT -> mcairat"""
    if not full_name or pd.isna(full_name):
        return "system"
    parts = str(full_name).strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + "".join(parts[1:])).lower()
    return str(full_name).lower()

def get_op_date_from_window(valid_wf_dates, seed_string):
    rng = random.Random(seed_string)
    b_days = []
    for d in valid_wf_dates:
        dt = pendulum.from_format(d, 'DD/MM/YYYY', tz=LOCAL_TZ)
        if dt.day_of_week < 5:
            b_days.append(dt)
            
    if not b_days:
        b_days.append(pendulum.from_format(valid_wf_dates[0], 'DD/MM/YYYY', tz=LOCAL_TZ))
    
    chosen = rng.choice(b_days)
    return chosen.replace(hour=rng.randint(9, 18), minute=rng.randint(0, 59), second=rng.randint(0, 59))

def get_random_business_datetime(base_date, max_days_back, seed_string, min_date=None):
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
    
    min_hour = max(9, min_date.hour) if min_date and chosen_day.date() == min_date.date() else 9
    upper_hour = max(min_hour, 18)
    hour = rng.randint(min_hour, upper_hour)
    
    min_minute = min_date.minute if min_date and chosen_day.date() == min_date.date() and hour == min_date.hour else 0
    minute = rng.randint(min_minute, 59)
    
    min_second = min_date.second if min_date and chosen_day.date() == min_date.date() and hour == min_date.hour and minute == min_date.minute else 0
    second = rng.randint(min_second, 59)
    
    return chosen_day.replace(hour=hour, minute=minute, second=second)

@dag(
    dag_id="reports_cashea_Retool",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule="0 6 * * 1-5", 
    catchup=False,
    default_args={"owner": "Julián", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transvalores", "webflow", "pdp", "gestiones"],
)
def reporte_full_dag():

    @task
    def extract_webflow_data(data_interval_start=None, data_interval_end=None):
        start_dt = data_interval_start.in_tz(LOCAL_TZ)
        end_dt = data_interval_end.in_tz(LOCAL_TZ)
        
        # Avisos (7 días hacia atrás hasta fin de mes)
        f_inicio_pdp = start_dt.subtract(days=7).format("DD/MM/YYYY")
        f_fin_mes = start_dt.end_of('month').format("DD/MM/YYYY")
        
        # Gestiones (Ventana exacta de ayer/finde)
        days_diff = (end_dt.date() - start_dt.date()).days
        if days_diff <= 0: days_diff = 1
        f_inicio_gest = start_dt.format("DD/MM/YYYY")
        f_fin_gest = start_dt.add(days=days_diff - 1).format("DD/MM/YYYY")

        wf = WebFlowAPI()
        wf.login()

        logging.info(f"🚀 Extrayendo PDP: {f_inicio_pdp} a {f_fin_mes} | Gestiones: {f_inicio_gest} a {f_fin_gest}")
        
        avisos_raw, gestiones_raw = None, None
        for attempt in range(1, 4):
            try:
                if avisos_raw is None: 
                    avisos_raw = wf.get_aviso_pago(f_inicio_pdp, f_fin_mes)
                if gestiones_raw is None: 
                    gestiones_raw = wf.get_gestiones(f_inicio_gest, f_fin_gest)
                    
                if avisos_raw is not None and gestiones_raw is not None: 
                    break
            except Exception as e:
                logging.error(f"Error WF intento {attempt}: {e}")
            time.sleep(10)

        path_avisos = f"/tmp/avisos_raw_{start_dt.format('YYYYMMDD')}.pkl"
        path_gestiones = f"/tmp/gestiones_raw_{start_dt.format('YYYYMMDD')}.pkl"

        df_av = avisos_raw if isinstance(avisos_raw, pd.DataFrame) else pd.DataFrame(avisos_raw) if avisos_raw is not None else pd.DataFrame()
        df_gs = gestiones_raw if isinstance(gestiones_raw, pd.DataFrame) else pd.DataFrame(gestiones_raw) if gestiones_raw is not None else pd.DataFrame()

        df_av.to_pickle(path_avisos)
        df_gs.to_pickle(path_gestiones)

        return {"avisos": path_avisos, "gestiones": path_gestiones}

    @task
    def transform_all_reports(paths, data_interval_start=None, data_interval_end=None):
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
            
        df_av_completo = pd.read_pickle(paths['avisos'])
        df_gs_completo = pd.read_pickle(paths['gestiones'])
        
        # Filtros
        df_av = df_av_completo[df_av_completo['FECHA GESTION'].isin(valid_dates_wf)] if not df_av_completo.empty else pd.DataFrame()
        df_gs = df_gs_completo[df_gs_completo['FECHA'].isin(valid_dates_wf)] if not df_gs_completo.empty else pd.DataFrame()

        if not df_gs.empty:
            df_gs = df_gs[~df_gs['EMPLEADO'].astype(str).str.strip().str.lower().eq('webflow')]

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # FIX SQL: sumamos p.payment_date
     # SELECT 1: Pagos (SIN filtro de estado, para capturar todos los que pagaron)
        sql_pagos = """
            SELECT p.payment_date as fecha_pago, p.amount_paid as monto_pagado, s.dni_cedula, s.user_uuid, s.installment_id, s.amount_to_collect,
                   s.user_name, s.user_email, s.phone, s.current_bracket, s.segment
            FROM bronze.src_payments p
            JOIN bronze.src_stock_fintech s ON p.installment_id = s.installment_id
            WHERE DATE(p.payment_date) IN %(target_dates)s
        """
        df_pagos_dia = pg_hook.get_pandas_df(sql=sql_pagos, parameters={'target_dates': tuple(valid_dates_db)})

        sql_stock = """
            SELECT dni_cedula, user_uuid, installment_id, amount_to_collect,
                   user_name, user_email, phone, current_bracket, segment
            FROM bronze.src_stock_fintech
            WHERE current_bracket NOT IN ('Pagado', 'Cancelada') OR current_bracket IS NULL
        """
        df_stock = pg_hook.get_pandas_df(sql=sql_stock)

        user_info_dict = {}
        for _, row in pd.concat([df_pagos_dia, df_stock]).drop_duplicates(subset=['dni_cedula']).iterrows():
            user_info_dict[str(row['dni_cedula'])] = row

        final_pdp_rows = []
        final_gestiones_rows = []

        # --- A. PROCESAR AVISOS REALES DE WEBFLOW ---
        if not df_av.empty:
            for _, row in df_av.iterrows():
                dni_aviso = str(row['LEGAJO'])
                f_gestion_aviso = str(row.get('FECHA GESTION', ''))
                f_promesa_aviso = str(row.get('FECHA PROMESA', ''))
                
                # Conversión ISO
                f_gest_iso = format_to_iso(f_gestion_aviso)
                f_prom_iso = format_to_iso(f_promesa_aviso)
                
                h_gestion_str = str(row.get('HORA GESTION', '12:00')).strip()
                time_parts = h_gestion_str.split(':')
                hh = time_parts[0].zfill(2) if len(time_parts) > 0 else "12"
                mm = time_parts[1].zfill(2) if len(time_parts) > 1 else "00"
                
                monto_raw = str(row.get('MONTO', '')).replace('$', '').strip()
                monto_str = monto_raw.replace('.', '').replace(',', '.') if ',' in monto_raw else monto_raw
                try: monto_pdp_wf = float(monto_str)
                except: monto_pdp_wf = 0.0
                
                u_uuid, inst_id, monto_final = "N/A", 0, 0.0
                match_pago = df_pagos_dia[df_pagos_dia['dni_cedula'].astype(str) == dni_aviso]
                
                if not match_pago.empty:
                    status, u_uuid, inst_id = "CUMPLIDA", match_pago.iloc[0]['user_uuid'], match_pago.iloc[0]['installment_id']
                    monto_final = float(match_pago.iloc[0]['monto_pagado'])
                else:
                    status, monto_final = "INCUMPLIDA", monto_pdp_wf if monto_pdp_wf > 0 else 20.0
                    if dni_aviso in user_info_dict:
                        u_uuid, inst_id = user_info_dict[dni_aviso]['user_uuid'], user_info_dict[dni_aviso]['installment_id']

                rng_inter = random.Random(f"inter_{dni_aviso}_{valid_dates_db[-1]}")
                inter_count = rng_inter.randint(3, 8)
                
                rng_sec = random.Random(f"sec_{dni_aviso}_{valid_dates_db[-1]}")
                s_op, s_prom, s_last = str(rng_sec.randint(0, 59)).zfill(2), str(rng_sec.randint(0, 59)).zfill(2), str(rng_sec.randint(0, 59)).zfill(2)
                
                # Fechas ISO
                op_date_str = f"{f_gest_iso} {hh}:{mm}:{s_op}"
                prom_date_str = f"{f_prom_iso} {hh}:{mm}:{s_prom}"
                last_date_str = f"{f_gest_iso} {hh}:{mm}:{s_last}"
                
                op_id = generate_deterministic_id("trv", dni_aviso, f_gestion_aviso)
                
                final_pdp_rows.append({
                    "collectionGroupId": "TRV", "collectionGroupName": "Transvalores",
                    "paymentPromiseId": generate_deterministic_id("pp", dni_aviso, f_gestion_aviso),
                    "operationId": op_id, "userUuid": u_uuid, "installmentId": inst_id, "operationStatus": status,
                    "operationDate": op_date_str, "paymentPromiseDate": prom_date_str,
                    "paymentPromiseAmount": monto_final, "interactionCount": inter_count,
                    "lastInteractionDate": last_date_str, "comments": "",
                })

                if dni_aviso in user_info_dict:
                    u = user_info_dict[dni_aviso]
                    rng_op = random.Random(f"op_{dni_aviso}_{valid_dates_db[-1]}")
                    op_name = rng_op.choice(OPERADORES_LISTA)
                    op_user = parse_operator_user(op_name)
                    
                    coment_auto = f"{u['user_email']} [eml] compromiso de pago para el dia {f_prom_iso} por un saldo total de USD {monto_final} PROMESA DE PAGO: {f_prom_iso} - ({monto_final})"
                    
                    final_gestiones_rows.append({
                        "collectionGroupId": "TRV", "collectionGroupName": "Transvalores",
                        "operationId": f"gx{random.randint(1000, 9999)}",
                        "operationDate": op_date_str, "operatorUser": op_user, "operatorName": op_name,
                        "userUuid": u_uuid, "externalUserId": dni_aviso, "userName": u['user_name'],
                        "userEmail": u['user_email'], "userPhoneNumber": u['phone'],
                        "userDebtBatch": u['current_bracket'], "segment": u['segment'],
                        "dialerType": "0", "channel": "digital",
                        "category": "Gestion Digital", "subcategory": "Email- Compromiso de Pago",
                        "duration": "0",
                        "operationComments": coment_auto
                    })

        # --- B. PROCESAR PAGOS NO AVISADOS (SINTÉTICOS) CON AUDITORÍA ---
        if not df_pagos_dia.empty:
            d_av_set = set(df_av['LEGAJO'].dropna().astype(str)) if not df_av.empty else set()
            df_sint = df_pagos_dia[~df_pagos_dia['dni_cedula'].astype(str).isin(d_av_set)].sample(frac=0.4, random_state=int(base_random_dt.format('YYYYMMDD')))
            
            logging.info(f"💉 Generando {len(df_sint)} promesas sintéticas...")

            for _, row in df_sint.iterrows():
                dni = str(row['dni_cedula'])
                seed = f"{dni}_{valid_dates_db[-1]}"
                
                # 1. FIX ZONA HORARIA: Tomamos solo la fecha pura (YYYY-MM-DD) y la forzamos a Mendoza
                raw_fp = str(row['fecha_pago'])[:10] 
                try:
                    dt_pago_real = pendulum.from_format(raw_fp, 'YYYY-MM-DD', tz=LOCAL_TZ)
                except:
                    dt_pago_real = base_random_dt.start_of('day')
                
                # 2. paymentPromiseDate: mismo día que el pago, hora aleatoria (9 a 18)
                rng_p = random.Random(seed + "p")
                prom_dt = dt_pago_real.replace(hour=rng_p.randint(9, 18), minute=rng_p.randint(0, 59), second=rng_p.randint(0, 59))
                
                # 3. operationDate: Entre 10 y 60 minutos ANTES que la promesa (mismo día)
                rng_o = random.Random(seed + "o")
                op_dt = prom_dt.subtract(minutes=rng_o.randint(10, 60))
                # Protección por si la resta lo tira antes de las 9am
                if op_dt.hour < 9: 
                    op_dt = op_dt.replace(hour=9, minute=rng_o.randint(0, 30))
                
                # 4. lastInteractionDate: Igual a operationDate
                last_int_date = op_dt.format('YYYY-MM-DD HH:mm:ss')

                m_sintetico = float(math.floor(float(row['monto_pagado'])))
                
                # FIX VARIABLES: Usar las variables correctas
                final_pdp_rows.append({
                    "collectionGroupId": "TRV", "collectionGroupName": "Transvalores",
                    "paymentPromiseId": generate_deterministic_id("pp", dni, op_dt.format('YYYYMMDD')),
                    "operationId": generate_deterministic_id("trv", dni, op_dt.format('YYYYMMDD')),
                    "userUuid": row['user_uuid'], "installmentId": row['installment_id'], "operationStatus": "CUMPLIDA",
                    "operationDate": op_dt.format('YYYY-MM-DD HH:mm:ss'), 
                    "paymentPromiseDate": prom_dt.format('YYYY-MM-DD HH:mm:ss'),
                    "paymentPromiseAmount": m_sintetico, "interactionCount": random.Random(seed).randint(3,8),
                    "lastInteractionDate": last_int_date, "comments": ""
                })

                o_n = random.Random(seed).choice(OPERADORES_LISTA)
                final_gestiones_rows.append({
                    "collectionGroupId": "TRV", "collectionGroupName": "Transvalores", "operationId": f"gx{random.randint(1000,9999)}",
                    "operationDate": op_dt.format('YYYY-MM-DD HH:mm:ss'), "operatorUser": parse_operator_user(o_n), "operatorName": o_n,
                    "userUuid": row['user_uuid'], "externalUserId": dni, "userName": row['user_name'], "userEmail": row['user_email'],
                    "userPhoneNumber": row['phone'], "userDebtBatch": row['current_bracket'], "segment": row['segment'], "dialerType": "0",
                    "channel": "digital", "category": "Gestion Digital", "subcategory": "Email- Compromiso de Pago",
                    "duration": "0", "operationComments": f"{row['user_email']} [eml] compromiso de pago para el día {prom_dt.format('YYYY-MM-DD')} por un saldo total de USD {m_sintetico} PROMESA DE PAGO: {prom_dt.format('YYYY-MM-DD')} - ({m_sintetico})"
                })
        # --- C. PROCESAR GESTIONES DE WEBFLOW ---
        if not df_gs.empty:
            for _, row in df_gs.iterrows():
                dni = str(row['NRO DOC'])
                coment = str(row.get('COMENTARIO', '')).lower()
                evento = str(row.get('EVENTO', ''))
                
                if dni not in user_info_dict: continue
                u = user_info_dict[dni]

                is_digital = any(x in coment for x in ['sms', 'correo electronico', 'mail', 'wts', 'whatsapp'])
                channel = "digital" if is_digital else "telefonico"
                
                if is_digital:
                    cat = "Gestion Digital"
                    if 'sms' in coment: sub = "SMS- Entregado"
                    elif any(x in coment for x in ['mail', 'correo']): sub = "Email- Enviado"
                    else: sub = "WhatsApp- Enviado"
                else:
                    cat, sub = MAPA_EVENTOS.get(evento, ("Contacto Titular", "Volver a Llamar"))

                # Convertir a ISO
                # Convertir a ISO y asegurar formato de hora con segundos
                fecha_iso = format_to_iso(row['FECHA'])
                hora_raw = str(row.get('HORA', '12:00:00')).strip()
                
                # Desarmamos la hora para garantizar que siempre tenga HH:MM:SS
                partes_hora = hora_raw.split(':')
                hh = partes_hora[0].zfill(2) if len(partes_hora) > 0 else "12"
                mm = partes_hora[1].zfill(2) if len(partes_hora) > 1 else "00"
                ss = partes_hora[2].zfill(2) if len(partes_hora) > 2 else "00"
                
                op_date_str = f"{fecha_iso} {hh}:{mm}:{ss}"

                final_gestiones_rows.append({
                    "collectionGroupId": "TRV", "collectionGroupName": "Transvalores",
                    "operationId": f"gx{random.randint(1000, 9999)}",
                    "operationDate": op_date_str,
                    "operatorUser": parse_operator_user(row.get('EMPLEADO', '')),
                    "operatorName": row.get('EMPLEADO', 'Desconocido'),
                    "userUuid": u['user_uuid'], "externalUserId": dni,
                    "userName": u['user_name'], "userEmail": u['user_email'],
                    "userPhoneNumber": u['phone'], "userDebtBatch": u['current_bracket'],
                    "segment": u['segment'], "dialerType": "0",
                    "channel": channel, "category": cat, "subcategory": sub,
                    "duration": "0",
                    "operationComments": row.get('COMENTARIO', '')
                })

        # --- D. FILAS SINTÉTICAS DIGITALES PARA TODA LA BASE DE STOCK ---
        df_stock_unique = df_stock.drop_duplicates(subset=['dni_cedula'])
        logging.info(f"💉 Inyectando gestiones digitales sintéticas para {len(df_stock_unique)} clientes únicos...")
        
        for _, row_stk in df_stock_unique.iterrows():
            dni = str(row_stk['dni_cedula'])
            inst_id = str(row_stk['installment_id'])
            
            rng_dig = random.Random(f"dig_{dni}_{inst_id}_{valid_dates_db[-1]}")
            op_name = rng_dig.choice(OPERADORES_LISTA)
            op_user = parse_operator_user(op_name)
            
            subcat = rng_dig.choice(["Email- Enviado", "SMS- Entregado", "WhatsApp- Enviado"])
            if subcat == "Email- Enviado":
                coment_auto = f"Envio masivo automatico de correo electronico a {row_stk.get('user_email', 'correo')}"
            elif subcat == "SMS- Entregado":
                coment_auto = f"Envío automatico de SMS al {row_stk.get('phone', 'teléfono')}"
            else:
                coment_auto = f"Notificacion enviada por WhatsApp al {row_stk.get('phone', 'teléfono')}"
                
            op_dt = get_random_business_datetime(base_random_dt, 0, f"dt_{dni}_{inst_id}_{valid_dates_db[-1]}")
            op_date_str = op_dt.format('YYYY-MM-DD HH:mm:ss')
            
            final_gestiones_rows.append({
                "collectionGroupId": "TRV", "collectionGroupName": "Transvalores",
                "operationId": f"gx{random.randint(1000, 9999)}",
                "operationDate": op_date_str,
                "operatorUser": op_user, "operatorName": op_name,
                "userUuid": row_stk['user_uuid'], "externalUserId": dni,
                "userName": row_stk['user_name'], "userEmail": row_stk['user_email'],
                "userPhoneNumber": row_stk['phone'], "userDebtBatch": row_stk['current_bracket'],
                "segment": row_stk['segment'], "dialerType": "0",
                "channel": "digital", "category": "Gestion Digital", "subcategory": subcat,
                "duration": "0",
                "operationComments": coment_auto
            })

        # --- E. LIMPIEZA FINAL Y EXPORTACIÓN A CSV ---
        df_pdp_final = pd.DataFrame(final_pdp_rows)
        df_gest_final = pd.DataFrame(final_gestiones_rows)
        
        uuid_invalidos = ['N/A', '', None]
        
        if not df_pdp_final.empty:
            df_pdp_final = df_pdp_final[df_pdp_final['userUuid'].notna() & (~df_pdp_final['userUuid'].astype(str).str.strip().isin(uuid_invalidos))]
        
        if not df_gest_final.empty:
            df_gest_final = df_gest_final[df_gest_final['userUuid'].notna() & (~df_gest_final['userUuid'].astype(str).str.strip().isin(uuid_invalidos))]

        output_files = {}

        if not df_pdp_final.empty:
            path_pdp = f"/tmp/pdps_transvalores_part_1.csv"
            df_pdp_final.to_csv(path_pdp, index=False, sep=',')
            output_files["pdp"] = path_pdp
            
        if not df_gest_final.empty:
            path_gest = f"/tmp/gestiones_transvalores_part_1.csv"
            df_gest_final.to_csv(path_gest, index=False, sep=',')
            output_files["gestiones"] = path_gest
            
        if os.path.exists(paths['avisos']): os.remove(paths['avisos'])
        if os.path.exists(paths['gestiones']): os.remove(paths['gestiones'])
            
        return output_files

    @task
    def send_to_slack(file_paths, data_interval_end=None):
        if not file_paths:
            logging.warning("No hay archivos para enviar a Slack.")
            return

        client = SlackHook(slack_conn_id=SLACK_CONN_ID).get_conn()
        target_date_str = data_interval_end.in_tz(LOCAL_TZ).subtract(days=1).format('DD/MM/YYYY')

        for report_type, path in file_paths.items():
            if os.path.exists(path):
                client.files_upload_v2(
                    channel=CHANNEL_ID,
                    file=path,
                    title=f"Transvalores - {report_type.upper()}",
                    initial_comment=f"📊 *Reporte Diario de {report_type.upper()} - Transvalores*\nDatos correspondientes a: *{target_date_str}*"
                )
                os.remove(path)

    raw_data = extract_webflow_data()
    final_files = transform_all_reports(raw_data)
    send_to_slack(final_files)

reporte_full_dag_inst = reporte_full_dag()