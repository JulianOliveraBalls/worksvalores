import os
import logging
import pandas as pd
import pendulum
from datetime import datetime, timedelta
from tabulate import tabulate

from airflow.decorators import dag, task
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.exceptions import AirflowSkipException

# Imports de tus clases
from include.webflow_client import WebFlowAPI
from include.inceptia_client import InceptiaAPI

# --- CONFIGURACIÓN GLOBAL ---
BOT_ID_PAGOS = 505
SLACK_CONN_ID = "slack_conn"
CHANNEL_ID = "C09V4KUCYBG"
#OPERACIONES C0104DJRX5M
#TEST C09V4KUCYBG
BLACK_LIST = ["WEBFLOW", "AGUSTIN BIZZOTTO", "JOSEFINA MANGIAROTTI", "DISCADOR PREDICTIVO", "NAN", "MONTEMARSILVANA", "TAPIA PAULA ANDREA"]
BLACKLIST_EVENTOS = ["NotificaciÛn PBX", "MANIPULACION MASIVA"]
OBJETIVO_DIARIO = 1500000

FERIADOS_2026 = [
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-03-24", 
    "2026-04-02", "2026-04-03", "2026-05-01", "2026-05-25", 
    "2026-06-15", "2026-06-20", "2026-07-09", "2026-08-17", 
    "2026-10-12", "2026-11-23", "2026-12-08", "2026-12-25"
]

MAPEO_CONTACTO = {
    'Aviso de Pago': 1, 'Convenio de Pago': 1, 'Contactado': 1,
    'Negociacion': 1, 'No Contactado': 0, 'Contacto familiar/ Laboral': 1,
    'Contacto Indirecto': 1, 'Mensaje en el Contestador': 0,
    'Cliente informa cancelación': 1, 'Entrante': 1, 'No Reconoce Deuda': 1,
    'Contactado - Sin trabajo': 1, 'Promesa de Pago': 1, 'Tel Erroneo': 0,
    'Fallecido': 1, 'Deudor Indiferente': 1, 'Deudor Indiferente Confirmado': 1,
    'Recordatorio Hecho': 1, 'Comentarios': 1, 'Teléfono Ocupado': 0,
    'Sin Datos': 0
}

# --- HELPERS ---
def clean_money(val):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return 0.0
    if isinstance(val, (int, float)): return float(val)
    s = str(val).replace("$", "").replace(".", "").replace(",", ".").strip()
    try: return float(s)
    except: return 0.0

def formatear_pesos(valor):
    return f"$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def es_habil(fecha):
    if fecha.day_of_week >= 5: return False 
    if fecha.to_date_string() in FERIADOS_2026: return False
    return True

@dag(
    dag_id="report_daily_operators",
    start_date=datetime(2026, 1, 1),
    schedule="0 12 * * 1-5",  # 12 UTC = 09:00 MZA
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Julián", "retries": 0},
    tags=["transvalores", "prod"],
)
def pagos_reporte_dag():

    @task
    def extract_data():
        tz = 'America/Argentina/Mendoza'
        hoy = pendulum.now(tz)
        #FECHA TEST. hoy = pendulum.datetime(2026, 2, 18, tz=tz)


        if not es_habil(hoy):
            raise AirflowSkipException(f"Hoy {hoy.to_date_string()} no es día hábil, skip.")

        gestion = hoy.subtract(days=1)
        while not es_habil(gestion):
            logging.info(f"⏭️ Saltando {gestion.to_date_string()} | day_of_week={gestion.day_of_week}")
            gestion = gestion.subtract(days=1)

        logging.info(f"🗓️ Hoy: {hoy.to_date_string()} | Gestión calculada: {gestion.to_date_string()}")

        f_gestion = gestion.format('DD/MM/YYYY')
        f_corte = hoy.format('DD/MM/YYYY')
        f_inicio_avisos = hoy.subtract(weeks=2).format('DD/MM/YYYY')
        f_fin_avisos = hoy.add(weeks=1).format('DD/MM/YYYY')
        ds_gestion = gestion.to_date_string()

        wf = WebFlowAPI()
        wf.login()
        
        data = {
            "gestiones": wf.get_gestiones(f_gestion, f_gestion),
            "convenios": wf.get_convenios_pago(f_gestion, f_gestion),
            "avisos": wf.get_aviso_pago(f_inicio_avisos, f_fin_avisos), 
            "llamados": wf.get_llamados(f_gestion, f_corte),
            "perdidas": wf.get_perdidas(f_gestion, f_gestion),
            "bots": InceptiaAPI().obtener_todos_los_casos(BOT_ID_PAGOS, ds_gestion, ds_gestion, estado="Interesado - Transferir"),
            "fecha_reportada": f_gestion
        }
        
        path = f"/tmp/bundle_{ds_gestion}.pkl"
        pd.to_pickle(data, path)
        return path

    @task
    def transform_and_report(path_bundle: str):
        import unicodedata
        data = pd.read_pickle(path_bundle)
        f_gestion = data.get('fecha_reportada', 'N/A')
        
        df_g = data.get('gestiones', pd.DataFrame())
        df_cv = data.get('convenios', pd.DataFrame())
        df_av = data.get('avisos', pd.DataFrame())
        df_ll = data.get('llamados', pd.DataFrame())
        df_lp = data.get('perdidas', pd.DataFrame())
        df_bots = data.get('bots', pd.DataFrame())

        client = SlackHook(slack_conn_id=SLACK_CONN_ID).get_conn()

        # --- NORMALIZACIÓN PARA BLACKLIST ---
        def normalizar_texto(s):
            if pd.isna(s): return ""
            s = str(s).upper().strip()
            s = s.replace('Ûn', 'O').replace('Û', 'O')
            s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
            return s

        def limpiar_n(df, col):
            if df.empty or col not in df.columns: return pd.Series(dtype='str')
            s = df[col].astype(str).str.replace(r'^\d{2}/\d{2}/\d{4}\d{2}:\d{2}', '', regex=True).str.strip().str.upper()
            return s.replace(['NAN', 'NONE', 'NULL', ''], pd.NA)

        # 1. Filtro Blacklist Eventos
        if not df_g.empty:
            df_g['EVENTO_NORM'] = df_g['EVENTO'].apply(normalizar_texto)
            bl_norm = [normalizar_texto(x) for x in BLACKLIST_EVENTOS]
            df_g = df_g[~df_g['EVENTO_NORM'].isin(bl_norm)].copy()

        # 2. Procesar Avisos
        if not df_av.empty:
            df_av = df_av[df_av['FECHA GESTION'] == f_gestion].copy()
            df_av['M_NUM'] = df_av['MONTO'].apply(clean_money)
            df_av = df_av[df_av['M_NUM'] > 3000]

        # 3. Lista Maestra Operadores
        activos = set()
        for df, col in [(df_g, 'EMPLEADO'), (df_cv, 'USUARIO CARGA'), (df_av, 'USUARIO GESTION'), (df_ll, 'nombre')]:
            activos.update(limpiar_n(df, col).dropna().unique())
        lista_final = sorted([e for e in activos if e not in BLACK_LIST and len(str(e)) > 2])
        df_res = pd.DataFrame(lista_final, columns=['EMPLEADO'])
        for c in ['qg', 'sc', 'qp', 'ms', 'qe', 'qs']: df_res[c] = 0.0

        # 4. Merges
        if not df_g.empty:
            df_g['EMP_L'] = limpiar_n(df_g, 'EMPLEADO')
            df_g['es_cont'] = df_g['EVENTO'].map(MAPEO_CONTACTO).fillna(0)
            res = df_g.groupby('EMP_L').agg(qg_v=('EVENTO', 'count'), sc_v=('es_cont', 'sum')).reset_index()
            df_res = df_res.merge(res, left_on='EMPLEADO', right_on='EMP_L', how='left')
            df_res['qg'] = df_res['qg_v'].fillna(0); df_res['sc'] = df_res['sc_v'].fillna(0)
            df_res = df_res.drop(columns=['EMP_L', 'qg_v', 'sc_v'], errors='ignore')

        for df, col_u, col_m in [(df_cv, 'USUARIO CARGA', 'MONTO CUOTA'), (df_av, 'USUARIO GESTION', 'MONTO')]:
            if not df.empty:
                df['EMP_L'] = limpiar_n(df, col_u)
                df['M_NUM'] = df[col_m].apply(clean_money)
                res = df.groupby('EMP_L').agg(qp_v=('M_NUM', 'count'), ms_v=('M_NUM', 'sum')).reset_index()
                df_res = df_res.merge(res, left_on='EMPLEADO', right_on='EMP_L', how='left')
                df_res['qp'] += df_res['qp_v'].fillna(0); df_res['ms'] += df_res['ms_v'].fillna(0)
                df_res = df_res.drop(columns=['EMP_L', 'qp_v', 'ms_v'], errors='ignore')

        q_sid = 0
        if not df_ll.empty:
            df_ll['EMP_L'] = limpiar_n(df_ll, 'nombre')
            mask = (df_ll['EMP_L'].isna()) | (~df_ll['EMP_L'].isin(lista_final) & ~df_ll['EMP_L'].isin(BLACK_LIST))
            q_sid = len(df_ll[mask & df_ll['tipo'].str.lower().str.contains('entrante')])
            e = df_ll[df_ll['tipo'].str.lower().str.contains('entrante')].groupby('EMP_L').size().reset_index(name='qe_v')
            s = df_ll[df_ll['tipo'].str.lower().str.contains('saliente')].groupby('EMP_L').size().reset_index(name='qs_v')
            df_res = df_res.merge(e, left_on='EMPLEADO', right_on='EMP_L', how='left').merge(s, left_on='EMPLEADO', right_on='EMP_L', how='left')
            df_res['qe'] = df_res['qe_v'].fillna(0); df_res['qs'] = df_res['qs_v'].fillna(0)
            df_res = df_res.drop(columns=['EMP_L_x', 'EMP_L_y', 'qe_v', 'qs_v'], errors='ignore')

        # 5. Formateo y Totales
        df_res['% Contacto'] = (df_res['sc'] / df_res['qg']).apply(lambda x: f"{x:.2%}" if x > 0 else "0.00%")
        df_res['Monto Promesas'] = df_res['ms'].apply(formatear_pesos)
        df_res['% Obj'] = (df_res['ms'] / OBJETIVO_DIARIO).apply(lambda x: f"{x:.2%}")

        reporte = df_res[['EMPLEADO', 'qg', '% Contacto', 'qp', 'Monto Promesas', '% Obj', 'qs', 'qe']].copy()
        reporte.columns = ['EMPLEADO', 'Q Gestiones', '% Contacto', 'Q_promesas', 'Monto Promesas', '% Obj', 'Q sal', 'Q ent']
        reporte = reporte.sort_values(by='Q Gestiones', ascending=False)

        fila_sid = pd.DataFrame([{'EMPLEADO': 'Sin ID', 'Q ent': q_sid}])
        total_gest = reporte['Q Gestiones'].sum()
        fila_tot = pd.DataFrame([{
            'EMPLEADO': 'TOTAL', 'Q Gestiones': total_gest,
            '% Contacto': f"{(df_res['sc'].sum() / total_gest):.2%}" if total_gest > 0 else "0.00%",
            'Q_promesas': df_res['qp'].sum(), 'Monto Promesas': formatear_pesos(df_res['ms'].sum()),
            '% Obj': f"{(df_res['ms'].sum() / (OBJETIVO_DIARIO * len(reporte))):.2%}" if len(reporte) > 0 else "0.00%",
            'Q sal': df_res['qs'].sum(), 'Q ent': df_res['qe'].sum() + q_sid
        }])

        df_final = pd.concat([reporte, fila_sid, fila_tot], ignore_index=True).fillna("")

        # Limpieza de decimales
        for col in df_final.columns:
            df_final[col] = df_final[col].apply(lambda x: str(x).replace('.0', '') if isinstance(x, (float, int)) or (isinstance(x, str) and x.endswith('.0')) else x)

# 6. Envío Reporte Principal
        tabla = tabulate(df_final, headers='keys', tablefmt='presto', showindex=False)
        msg = f"📊 *Resumen de Gestión Diaria - {f_gestion}*\n```\n{tabla}\n```"
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)

# 7. Inceptia - Leads Pendientes (Formato Snippet de Slack)
        if not df_bots.empty:
            dnis_gestionados = set()
            if not df_g.empty and 'NRO DOC' in df_g.columns:
                dnis_gestionados = set(df_g['NRO DOC'].astype(str).str.strip().unique())
            
            df_bots['DNI_B'] = df_bots['params.code'].astype(str).str.strip()
            df_pend = df_bots[~df_bots['DNI_B'].isin(dnis_gestionados)].copy()
            
            if not df_pend.empty:
                df_pend = df_pend[['DNI_B', 'phone']].rename(columns={'DNI_B': 'DNI', 'phone': 'Teléfono'}).drop_duplicates()
                
                # Enumeración para el equipo
                df_pend = df_pend.reset_index(drop=True)
                df_pend.index = df_pend.index + 1
                df_pend = df_pend.reset_index().rename(columns={'index': '#'})
                
                # Convertimos a una tabla de texto simple para el snippet
                bot_table = tabulate(df_pend, headers='keys', tablefmt='plain', showindex=False)
                
                # ENVIAR COMO ARCHIVO (Snippet)
                try:
                    client.files_upload_v2(
                        channel=CHANNEL_ID,
                        content=bot_table,
                        filename=f"leads_pendientes_{f_gestion.replace('/','-')}.txt",
                        title=f"🤖 Leads Inceptia sin Gestión - {f_gestion}",
                    )
                except Exception as e:
                    logging.error(f"Error enviando snippet de Inceptia: {e}")
            else:
                client.chat_postMessage(channel=CHANNEL_ID, text=f"🤖 *Inceptia:* Todos los interesados del {f_gestion} ya fueron gestionados.")
        else:
            client.chat_postMessage(channel=CHANNEL_ID, text=f"🤖 No hubo llamados de IA el {f_gestion}.")

        # Limpieza de archivo temporal
        if os.path.exists(path_bundle): 
            os.remove(path_bundle)

    # --- FLUJO DE EJECUCIÓN ---
    bundle_path = extract_data()
    transform_and_report(bundle_path)

# Instanciación final
pagos_reporte_dag_inst = pagos_reporte_dag()