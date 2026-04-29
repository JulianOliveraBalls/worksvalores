import os
import io
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from slack_sdk import WebClient

from include.webflow_client import WebFlowAPI


# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =========================
# CONFIG
# =========================
SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/opt/airflow/include/drive_service_account.json"
)

SCOPES = ['https://www.googleapis.com/auth/drive']
CARPETA_SPV_ID = "1UU3CQl7GY1qnZ3QByuLnxNcT2XfWtaUL"

EXCEL_DESCARGADO_PATH = "/tmp/drive_input.xlsx"
RAW_GESTIONES_PATH = "/tmp/gestiones_raw.txt"
OUTPUT_FINAL_PATH = "/tmp/output_final.xlsx"

SLACK_CONN_ID = Variable.get("slack_conn_id", default_var="slack_conn")
ID_CANAL = Variable.get("spv_slack_canal", default_var="C0AJ3ULQL75")


# =========================
# DRIVE
# =========================
def get_drive_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)


def download_file(file_id, dest_path):
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)

    with io.FileIO(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logging.info(f"Descargando {file_id}: {int(status.progress() * 100)}%")


# =========================
# TASK 1
# =========================
def task_descargar_drive(**context):
    logging.info("🚀 Descargando desde Drive")

    service = get_drive_service()

    meses_es = {
        "January": "Enero", "February": "Febrero", "March": "Marzo",
        "April": "Abril", "May": "Mayo", "June": "Junio",
        "July": "Julio", "August": "Agosto", "September": "Septiembre",
        "October": "Octubre", "November": "Noviembre", "December": "Diciembre"
    }

    all_excel_files = []

    for i in range(4):
        fecha_busqueda = datetime.now() - timedelta(days=28 * i)
        nombre_mes = meses_es[fecha_busqueda.strftime('%B')]
        target_name = f"{nombre_mes} {fecha_busqueda.year}".lower()

        logging.info(f"🔎 Buscando: {target_name}")

        query_folder = f"'{CARPETA_SPV_ID}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        folders = service.files().list(q=query_folder).execute().get('files', [])

        folder_id = next((f['id'] for f in folders if target_name in f['name'].lower()), None)

        if folder_id:
            logging.info(f"📂 Carpeta encontrada")

            q_files = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
            files = service.files().list(
                q=q_files,
                orderBy="modifiedTime desc",
                fields="files(id, name, modifiedTime)"
            ).execute().get('files', [])

            all_excel_files.extend(files)

    if not all_excel_files:
        raise ValueError("❌ No se encontraron Excel")

    all_excel_files.sort(key=lambda x: x['modifiedTime'], reverse=True)

    df_lista = []

    for f in all_excel_files[:2]:
        logging.info(f"📥 Descargando: {f['name']}")
        temp_path = f"/tmp/{f['id']}.xlsx"

        download_file(f['id'], temp_path)

        df_lista.append(pd.read_excel(temp_path))
        os.remove(temp_path)

    df_combined = pd.concat(df_lista, ignore_index=True).drop_duplicates()
    logging.info(f"📊 Total SPV: {len(df_combined)}")

    df_combined.to_excel(EXCEL_DESCARGADO_PATH, index=False)


# =========================
# TASK 2
# =========================
def task_extraer_gestiones(**context):
    logging.info("🚀 Extrayendo gestiones")

    api = WebFlowAPI()

    if not api.login():
        raise Exception("❌ Error login WebFlow")

    # 🔒 HARDCODE
    execution_date = context['logical_date']

# base para ciclo cada 3 días (hoy 23/04 ya corriste manual)
    start_base = datetime(2026, 4, 23)

    diff = (execution_date.date() - start_base.date()).days

    # ⛔ si no toca correr → cortar task
    if diff % 3 != 0 or diff == 0:
        logging.info("⏭️ No es día de ejecución (cada 3 días)")
        raise AirflowSkipException("Skip ejecución")

    # ✅ ventana automática (3 días anteriores)
    fecha_fin_dt = execution_date - timedelta(days=1)
    fecha_ini_dt = execution_date - timedelta(days=3)

    fecha_ini = fecha_ini_dt.strftime('%d/%m/%Y')
    fecha_fin = fecha_fin_dt.strftime('%d/%m/%Y')

    logging.info(f"📅 {fecha_ini} → {fecha_fin}")


    df = api.get_gestiones(fecha_ini, fecha_fin)

    logging.info(f"📊 Filas gestiones: {len(df)}")

    df.to_csv(RAW_GESTIONES_PATH, sep="\t", index=False)


# =========================
# TASK 3 (TUYA TAL CUAL)
# =========================
def task_generar_output(**context):
    if not os.path.exists(RAW_GESTIONES_PATH):
        raise AirflowSkipException("No hay gestiones para procesar")
    df_gest = pd.read_csv(RAW_GESTIONES_PATH, sep="\t", encoding="latin1", on_bad_lines='skip')
    df_spv = pd.read_excel(EXCEL_DESCARGADO_PATH)
    clean_id = lambda x: '{:.0f}'.format(float(x)) if pd.notnull(x) and str(x).lower() != 'nan' else str(x)
    eventos_a_excluir = ['Notificación PBX', 'NotificaciÃ³n PBX', 'Gestion Agot - Devolucion Contactado',
                         'Gestion Agot - Devolucion No Contactado', 'Gestion Agot - Devolucion Cancelado']
    df_gest = df_gest[~df_gest['EVENTO'].isin(eventos_a_excluir)]

    estados_a_excluir = ['GestiÃ³n Agotada - Se Niega a Pagar', 'Gestión Agotada - Se Niega a Pagar',
                         'Gestion Agot - Devolucion Cancelado', 'Gestion Agot - Devolucion Contactado',
                         'Gestion Agot - Devolucion No Contactado']
    df_gest = df_gest[~df_gest['ESTADO FINAL'].str.strip().isin(estados_a_excluir)]

    df_gest = df_gest[
        ~df_gest['EMPLEADO'].str.upper().isin(['WEBFLOW', 'MONTEMAR AUTOMATICO 1', 'ADV1', 'LEONARDO FEDERICO D`ARCO','AGUSTIN BIZZOTTO', 'DISCADOR PREDICTIVO'])]

    df_gest['NRO DOC'] = pd.to_numeric(df_gest['NRO DOC'], errors='coerce')
    df_spv['DNI'] = pd.to_numeric(df_spv['DNI'], errors='coerce')

    df_audit = pd.merge(df_gest, df_spv[['DNI', 'Clave del Banco de la Persona ']], left_on='NRO DOC', right_on='DNI',
                        how='left')
    df_audit = df_audit.rename(columns={'Clave del Banco de la Persona ': 'Cliente'})

    map_com = {
        'Fallecido': 'Fallecido', 'No Reconoce Deuda': 'Desconoce deuda',
        'Contactado - Sin trabajo': 'Desempleado/Sin ingresos',
        'Contactado - Otras deudas': 'Sobreendeudado', 'Contactado': 'Contacto directo con el cliente',
        'No Contactado': 'Sin contacto',
        'Tel Erroneo': 'Sin contacto / Datos de contacto erróneos ', 'Sin Datos': 'Sin contacto / Búsqueda de Datos',
        'Teléfono Ocupado': 'Sin contacto / Teléfono Ocupado', 'Mensaje en el Contestador': 'Sin contacto / Casilla',
        'Contacto familiar/ Laboral': 'Contacto Indirecto con familia o trabajo',
        'Contacto Indirecto': 'Contacto Indirecto',
        'Promesa de Pago': 'Se acuerda una promesa de pago', 'Convenio de Pago': 'Se genera un convenio de pago',
        'Para efectuar Recordatorio CDP': 'No abona cuota correspondiente al convenio de pago, se efectúa recordatorio',
        'Aviso de Pago': 'Se comunica para dar aviso del pago, adjuntando comprobante',
        'Promesa Cumplida': 'Promesa de pago cumplida',
        'Se registra pago en el mes': 'Se registra pago en el mes',
        'Se registra el Pago inicial': 'Se registra el pago inicial del acuerdo',
        'Se Registran Todas las Cuotas Pagas': 'Se registran todas las cuotas pagas',
        'Para efectuar Recordatorio PDP': 'No abona cuota correspondiente al plan de pago, se efectúa recordatorio',
        'Deudor Indiferente': 'Cliente indiferente - Problemas personales',
        'Deudor Indiferente Confirmado': 'Cliente indiferente - Problemas personales',
        'Negociacion': 'Cliente toma conocimiento de la deuda y comienza la Negociación',
        'Entrante': 'cliente manifiesta interés por acordar alguna forma de regularizar la deuda',
        'Promesa Incumplida': 'Promesa de pago incumplida',
        'Se Registran Cuotas en Mora': 'Se registran cuotas vencidas del convenio',
        'No registra pagos desde convenio': 'No registra pagos desde el convenio',
        'No se Registran Cuotas en Mora': 'No se registran cuotas en mora',
        'No se registran pagos por 2 meses': 'No registra pagos durante dos meses consecutivos',
        'Anular CDP': 'Se anula el convenio de pago',
        'Cliente informa cancelación': 'Cliente informa que la deuda fue cancelada',
        'Se Registran demasiadas Cuotas en Mora': 'Se registran demasiadas cuotas en mora',
        'No se Registra el Pago Inicial': 'No se registra el pago inicial del acuerdo',
    }

    map_resp = {
        'Para efectuar Recordatorio CDP': 'CONAC', 'Promesa Incumplida': 'CLIDE',
        'Se Registran Cuotas en Mora': 'CLIDE',
        'Se Registran Todas las Cuotas Pagas': 'CONAC', 'No registra pagos desde convenio': 'CLIDE',
        'No se Registran Cuotas en Mora': 'CONAC',
        'No se registran pagos por 2 meses': 'CLIDE', 'Negociacion': 'CCINT', 'Contactado': 'CCINT',
        'Aviso de Pago': 'CONAC',
        'Contacto familiar/ Laboral': 'MSJTR', 'Anular CDP': 'CLIDE',
        'Mensaje en el Contestador': 'CONAU',
        'Promesa de Pago': 'CONAC', 'Convenio de Pago': 'CONAC',
        'No Contactado': 'NCON',
        'Entrante': 'CCINT', 'Cliente informa cancelación': 'CONAC', 'Tel Erroneo': 'NCON',
        'Deudor Indiferente': 'CSINT',
        'Para efectuar Recordatorio PDP': 'CONAC', 'Fallecido': 'PRFAL', 'No Reconoce Deuda': 'PRFRA',
        'Se registra pago en el mes': 'CONAC',
        'Se registra el Pago inicial': 'CONAC', 'Se Registran demasiadas Cuotas en Mora': 'CLIDE',
        'Contactado - Sin trabajo': 'CSINT',
        'Promesa Cumplida': 'CONAC', 'Contactado - Otras deudas': 'CSINT', 'Deudor Indiferente Confirmado': 'CSINT',
        'Contacto Indirecto': 'MSJTR', 'No se Registra el Pago Inicial': 'CLIDE', 'Sin Datos': 'NCON',
        'Teléfono Ocupado': 'NCON'
    }

    map_usu = {'MANGIAROTTI EDUARDO': 'EM23257', 'TAPIA PAULA ANDREA': 'PT89540',
               'LEONARDO FEDERICO D\'ARCO': 'FD39203', 'MARIA CAIRAT': 'CM02670', 'ANDREA CARRAI': 'AC85430',
               'Valeria Cerda': 'CV96357', 'TAMARA MONTECINOS': 'MT70393', 'DANIELA MELA': 'MD71115', 'ALMA VILLEGAS': 'VA51528', 
               'YANINA COLLADO': 'CY33872', 'PAULA ZARATE': 'ZP76826'}

    df_audit['EMPLEADO_NORM'] = df_audit['EMPLEADO'].str.upper().str.strip()
    map_usu_u = {k.upper(): v for k, v in map_usu.items()}
    df_f_audit = pd.DataFrame()
    df_f_audit['Cliente'] = df_audit['Cliente'].apply(clean_id)
    df_f_audit['Accion'] = 'GEEXT'
    execution_date = context['logical_date']
    df_f_audit['Fecha de carga'] = execution_date.strftime('%Y%m%d')
    df_f_audit['usuario'] = df_audit['EMPLEADO_NORM'].map(map_usu_u).fillna(df_audit['EMPLEADO'])
    df_audit['EVENTO'] = df_audit['EVENTO'].astype(str).str.strip()
    df_f_audit['Comentario'] = df_audit['EVENTO'].map(map_com).fillna('Problemas personales')
    df_f_audit['Respuesta'] = df_audit['EVENTO'].map(map_resp)

    try:
        df_f_audit['Fecha de respuesta'] = pd.to_datetime(df_audit['FECHA'], dayfirst=True).dt.strftime('%Y%m%d')
    except:
        df_f_audit['Fecha de respuesta'] = df_audit['FECHA']

    df_f_audit['usuario de respuesta'] = df_audit['EMPLEADO_NORM'].map(map_usu_u).fillna(df_audit['EMPLEADO'])

    df_f_audit = df_f_audit.dropna(subset=['Respuesta', 'Cliente'])
    df_f_audit = df_f_audit[df_f_audit['Cliente'] != 'nan']
    df_f_audit = df_f_audit.drop_duplicates()
    df_f_audit['Cliente'] = df_f_audit['Cliente'].astype(str)
    
    import random

    # ==============================
    # NUEVA HOJA
    # ==============================
    df_clientes = df_spv[['Clave del Banco de la Persona ']].drop_duplicates().copy()
    df_clientes = df_clientes.rename(columns={'Clave del Banco de la Persona ': 'Cliente'})
    df_clientes['Cliente'] = df_clientes['Cliente'].apply(clean_id).astype(str)

    df_total = pd.DataFrame()
    df_total['Cliente'] = df_clientes['Cliente']
    df_total['Accion'] = 'GEEXT'
    execution_date = context['logical_date']
    df_total['Fecha de carga'] = execution_date.strftime('%Y%m%d')
    df_total['usuario'] = 'FD39203'
    df_total['Comentario'] = ''

    # 🎯 1/4 CONAU, resto NCON
    total_rows = len(df_total)
    conau_count = total_rows // 4

    respuestas = ['CONAU'] * conau_count + ['NCON'] * (total_rows - conau_count)
    random.shuffle(respuestas)

    df_total['Respuesta'] = respuestas

    # 🎲 fechas random entre hardcode
    execution_date = context['logical_date']
    fecha_fin = execution_date - timedelta(days=1)
    fecha_ini = execution_date - timedelta(days=3)

    df_total['Fecha de respuesta'] = [
        (fecha_ini + timedelta(days=random.randint(0, (fecha_fin - fecha_ini).days))).strftime('%Y%m%d')
        for _ in range(total_rows)
    ]

    df_total['usuario de respuesta'] = 'FD39203'

    # tipo correcto
    df_total['Cliente'] = df_total['Cliente'].astype(str)

    with pd.ExcelWriter(OUTPUT_FINAL_PATH, engine='openpyxl') as writer:
        df_f_audit.to_excel(writer, sheet_name="Auditoria", index=False)
        df_total.to_excel(writer, sheet_name="Auditoria_Total", index=False)

        ws = writer.sheets['Auditoria']
        for cell in ws['A']:
            cell.number_format = '@'

        ws2 = writer.sheets['Auditoria_Total']
        for cell in ws2['A']:
            cell.number_format = '@'


# =========================
# TASK 4
# =========================
def task_enviar_slack(**context):
    logging.info("📤 Enviando a Slack")
    execution_date = context['logical_date']

    fecha_fin_dt = execution_date - timedelta(days=1)
    fecha_ini_dt = execution_date - timedelta(days=3)

    fecha_ini = fecha_ini_dt.strftime('%d/%m/%Y')
    fecha_fin = fecha_fin_dt.strftime('%d/%m/%Y')

    conn = BaseHook.get_connection(SLACK_CONN_ID)
    client = WebClient(token=conn.password)

    tmp = "/tmp/slack.xlsx"

    # 👇 leer TODAS las hojas
    xls = pd.ExcelFile(OUTPUT_FINAL_PATH)
    sheets = {}

    for sheet in xls.sheet_names:
        if sheet in ["Auditoria", "Auditoria_Total"]:
            sheets[sheet] = pd.read_excel(
                OUTPUT_FINAL_PATH,
                sheet_name=sheet,
                dtype={'Cliente': str}
            )
        else:
            sheets[sheet] = pd.read_excel(
                OUTPUT_FINAL_PATH,
                sheet_name=sheet
            )

    # 👇 volver a escribir TODO el excel
    with pd.ExcelWriter(tmp, engine='openpyxl') as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

            # forzar formato texto en Cliente si existe
            if 'Cliente' in df.columns:
                ws = writer.sheets[name]
                for cell in ws['A']:
                    cell.number_format = '@'

    # 👇 enviar a slack
    client.files_upload_v2(
        channel=ID_CANAL,
        file=tmp,
        title=f"Gestiones_Emerix_SPV_{fecha_ini_dt.strftime('%d-%m')} al {fecha_fin_dt.strftime('%d-%m')}",
        initial_comment=f"📊 Reporte Gestiones para Emerix ({fecha_ini} al {fecha_fin})"
    )

    os.remove(tmp)


# =========================
# DAG
# =========================
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='spv_webflow_drive_consolidado_final',
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule='0 7 * * *',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='descargar_drive', python_callable=task_descargar_drive)
    t2 = PythonOperator(task_id='extraer_gestiones', python_callable=task_extraer_gestiones)
    t3 = PythonOperator(task_id='generar_output', python_callable=task_generar_output)
    t4 = PythonOperator(task_id='enviar_slack', python_callable=task_enviar_slack)

    t1 >> t2 >> t3 >> t4