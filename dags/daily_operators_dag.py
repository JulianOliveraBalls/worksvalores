from airflow import DAG
from airflow.utils import timezone
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
import io
import os
import pandas as pd
import numpy as np
import requests
import re
from io import BytesIO
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# =================================================================
# CONFIGURACIÓN CENTRAL DE FUENTES DE DATOS
# =================================================================

mes_actual = datetime.now().month
FUENTES_DE_DATOS = [
    {"nombre": "SUPERVIELLE", "tipo": "excel_dinamico", "carpeta_id": "1UU3CQl7GY1qnZ3QByuLnxNcT2XfWtaUL"},
    {"nombre": "UP", "tipo": "excel_dinamico", "carpeta_id": "1_tBSoHOT_N7A3b2TvBige4vQBboP6_U0"},
    {"nombre": "GARBARINO", "tipo": "excel_dinamico", "carpeta_id": "1WY_4WNwjo8J4NlqaoNLgqTrfea0kQMKZ"},
    {"nombre": "HIPOTECARIO", "tipo": "excel_dinamico", "carpeta_id": "1Vwifi_VZthcJ1krag4PSwBjOZ-JkfFaz"},
    {"nombre": "COLUMBIA", "tipo": "excel_dinamico", "carpeta_id": "1WRnYvE_v9s6yWgDU16rCvp04P08bWmeS"},
    {"nombre": "IXPANDIT", "tipo": "excel_dinamico", "carpeta_id": "1vO0xlakvMkw_KGXKmWox6mZN2e2yqpCP"},
    {"nombre": "PRESTER", "tipo": "excel_dinamico", "carpeta_id": "1XMja4ExH14vbbmdVmwgO_Ag_BMfGWZVK"},
    {"nombre": "UALA", "tipo": "excel_dinamico", "carpeta_id": "1X3Hj8fu_KcWh6LByIgvpLzJWUcTlzIq_"},
    {"nombre": "MERCADO-PAGO", "tipo": "excel_dinamico", "carpeta_id": "1m1CdUiF6mW-BS-KIVRmuQdFs4ltXb0yd"},
{"nombre": "IUDU", "tipo": "excel_dinamico", "carpeta_id": "1vIpOJhG2lBGCEjbaNdliBOPfmbkwnWxr"},

]

# =================================================================
# CONFIG GENERAL
# =================================================================
SERVICE_ACCOUNT_FILE = '/usr/local/airflow/include/drive_service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']
CARPETA_SPV_ID = "1UU3CQl7GY1qnZ3QByuLnxNcT2XfWtaUL"
EXCEL_MAESTRO_ID = "1-k-pn6AePbJRCOAdO01gRb_6EU75sdBK"
EXCEL_MAESTRO_PATH = "/tmp/Clientes_corrientes.xlsx"

# =================================================================
# CONFIGURACIÓN WEBFLOW (REQUERIDO PARA LOGIN)
# =================================================================
# --- ¡CAMBIOS APLICADOS! ---
# URLs y Headers para el LOGIN, basados en el DAG funcional (v7_6_prod)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
ORIGIN_URL = "https://cloud.webflow.com.ar"
# ESTA ES LA URL DE LA PÁGINA QUE CONTIENE EL FORMULARIO DE LOGIN
REFERER_URL = "https://cloud.webflow.com.ar/transvalores/html/log.html"  # <-- CORREGIDO
# ESTA ES LA URL A LA QUE EL FORMULARIO ENVÍA LOS DATOS (POST)
WEBFLOW_LOGIN_URL = "https://cloud.webflow.com.ar/transvalores/cgi-bin/log.cgi"  # <-- CORREGIDO (log.cgi)


# =================================================================
# HELPERS (Funciones de ayuda)
# =================================================================
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
            _, done = downloader.next_chunk()
    print(f"Archivo {file_id} descargado en {dest_path}")


def upload_file(file_id, local_path):
    service = get_drive_service()
    media = MediaFileUpload(local_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    file = service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
    print(f"✅ Archivo {local_path} actualizado en Drive (ID: {file['id']})")


# =================================================================
# DEFINICIÓN DE TAREAS DEL DAG
# =================================================================

def descargar_archivos_iniciales():
    print("--- 📥 Tarea 1: Descarga Dinámica desde Drive ---")
    os.makedirs("/tmp", exist_ok=True)
    download_file(EXCEL_MAESTRO_ID, EXCEL_MAESTRO_PATH)

    service = get_drive_service()
    ahora = datetime.now()
    meses_es = {
        "January": "Enero", "February": "Febrero", "March": "Marzo", "April": "Abril",
        "May": "Mayo", "June": "Junio", "July": "Julio", "August": "Agosto",
        "September": "Septiembre", "October": "Octubre", "November": "Noviembre", "December": "Diciembre"
    }
    nombre_mes = meses_es[ahora.strftime("%B")]
    anio_actual = ahora.year
    # Formato exacto: "FEBRERO 2026"
    folder_target_name = f"{nombre_mes} {anio_actual}"

    for fuente in FUENTES_DE_DATOS:
        path_destino = f"/tmp/{fuente['nombre']}.xlsx"
        print(f"🔍 Buscando carpeta '{folder_target_name}' en {fuente['nombre']}...")

        try:
            def buscar_recursivo(parent_id, target_name, nivel=1):
                query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                folders = service.files().list(q=query, supportsAllDrives=True,
                                               includeItemsFromAllDrives=True).execute().get('files', [])

                for f in folders:
                    # Coincidencia exacta o parcial para el mes año
                    if target_name.lower() in f['name'].lower():
                        return f['id'], f['name']
                    # Si es nivel 1 y es una carpeta tipo "Stocks 2026", entra
                    if nivel == 1 and (str(anio_actual) in f['name'] or "stock" in f['name'].lower()):
                        return buscar_recursivo(f['id'], target_name, nivel + 1)
                return None, None

            target_id, real_name = buscar_recursivo(fuente['carpeta_id'], folder_target_name)

            # Si no encontró carpeta del mes, intentamos en la raíz
            final_id = target_id if target_id else fuente['carpeta_id']
            if target_id: print(f"📂 Carpeta encontrada: {real_name}")

            # Buscar el Excel más nuevo
            q_excel = f"'{final_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
            res_excel = service.files().list(q=q_excel, orderBy="modifiedTime desc", fields="files(id, name)",
                                             supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get(
                'files', [])

            if res_excel:
                print(f"✅ Descargando: {res_excel[0]['name']}")
                download_file(res_excel[0]['id'], path_destino)
            else:
                print(f"❌ No se encontró Excel para {fuente['nombre']}")

        except Exception as e:
            print(f"❌ Error en {fuente['nombre']}: {e}")


def procesar_fuente(config):
    nombre_fuente = config['nombre']
    tipo_fuente = config['tipo']
    print(f"--- ⚙️ Procesando fuente: {nombre_fuente} ({tipo_fuente}) ---")
    df = None
    if tipo_fuente in ['excel_dinamico', 'excel_estatico']:
        path_archivo = f"/tmp/{nombre_fuente}.xlsx"
        df = pd.read_excel(path_archivo)

        if nombre_fuente == "SUPERVIELLE":
            print("Aplicando limpieza específica para SUPERVIELLE...")
            columnas_deseadas = ["DNI", "Apellido y Nombre", "Subsegmento", "Marca", "deuda total del cliente",
                                 "días de mora Total Cliente"]
            df = df[columnas_deseadas].copy()
            df["deuda total del cliente"] = pd.to_numeric(df["deuda total del cliente"], errors="coerce") / 100
            df["días de mora Total Cliente"] = pd.to_numeric(df["días de mora Total Cliente"], errors="coerce")
            df = df.drop_duplicates(subset=["DNI", "días de mora Total Cliente"])

            def clasificar_tramo(dias):
                if pd.isna(dias): return "Desconocido"
                if dias <= 120:
                    return "0 - 120"
                elif dias <= 180:
                    return "121 - 180"
                elif dias <= 360:
                    return "181 - 360"
                elif dias <= 720:
                    return "361 - 720"
                elif dias <= 1080:
                    return "721 - 1080"
                elif dias <= 1440:
                    return "1081 - 1440"
                elif dias <= 1800:
                    return "1441 - 1800"
                else:
                    return "Mas 1800"

            df["Tramo de Mora"] = df["días de mora Total Cliente"].apply(clasificar_tramo)
            df = df.rename(columns={"Apellido y Nombre": "Nombre"})

    if df is not None and not df.empty:
        if 'DNI' in df.columns:
            df['DNI'] = pd.to_numeric(df['DNI'], errors="coerce")
        print(f"--- ✅ Datos procesados para '{nombre_fuente}'. Devolviendo DataFrame. ---")
        return {"sheet_name": nombre_fuente, "dataframe": df.to_json(orient='split')}
    else:
        print(f"--- ⚠️ No se encontraron datos para la fuente {nombre_fuente}. ---")
        return None


def consolidar_resultados(**kwargs):
    print("--- ✍️ Consolidando todos los dataframes en el archivo maestro Excel ---")
    ti = kwargs['ti']
    # Recreamos los task_ids limpios para hacer el XCom pull
    task_ids_procesamiento = []
    for fuente in FUENTES_DE_DATOS:
        task_id_limpio = fuente['nombre'].replace(" ", "_")
        task_ids_procesamiento.append(f"procesamiento_de_fuentes.procesar_{task_id_limpio}")

    resultados_json = ti.xcom_pull(task_ids=task_ids_procesamiento)

    with pd.ExcelWriter(EXCEL_MAESTRO_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        for resultado in resultados_json:
            if resultado:
                nombre_hoja = resultado['sheet_name']
                df = pd.read_json(resultado['dataframe'], orient='split')
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
                print(f"--- ✅ Hoja '{nombre_hoja}' escrita en el archivo maestro. ---")
    print("--- ✅ Todas las hojas han sido consolidadas exitosamente. ---")


# --- TAREA DE LOGIN (CON URL Y REFERER CORREGIDOS) ---
def task_login_y_obtener_cookie():
    """
    Se conecta a WebFlow usando las credenciales de la conexión
    'webflow_prod' y retorna el diccionario de cookies de sesión.
    """
    print("--- 🔐 [TAREA DE LOGIN] Iniciando Tarea de Login en WebFlow ---")

    # 1. Obtener credenciales seguras desde Airflow Connections
    print("Obteniendo credenciales desde la Conexión 'webflow_prod' de Airflow...")
    try:
        conn = BaseHook.get_connection('webflow_prod')
        WEBFLOW_USER = conn.login
        WEBFLOW_PASS = conn.password
        if not WEBFLOW_USER or not WEBFLOW_PASS:
            raise ValueError("El 'Login' o 'Password' están vacíos en la conexión 'webflow_prod'.")
        print("Credenciales obtenidas exitosamente.")
    except Exception as e:
        print(f"❌ ERROR: No se pudo encontrar o leer la conexión 'webflow_prod'.")
        print(f"Asegúrate de haberla creado en Admin -> Connections.")
        raise e

    # 2. Replicar la lógica de tu script de login
    login_data = {
        'COUNT': '',
        'Op': '',
        'homePath': '',
        'USERNAME': WEBFLOW_USER,
        'CLAVE': WEBFLOW_PASS
    }

    # Headers específicos para el LOGIN
    login_headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": REFERER_URL,  # <-- constante corregida
        "Origin": ORIGIN_URL
    }

    print(f"Iniciando login en: {WEBFLOW_LOGIN_URL} para usuario: {WEBFLOW_USER}")  # <-- constante corregida

    try:
        with requests.Session() as session:
            print("Enviando petición POST para iniciar sesión...")
            resp_login = session.post(
                WEBFLOW_LOGIN_URL,  # <-- constante corregida
                headers=login_headers,
                data=login_data,
                timeout=30
            )
            resp_login.raise_for_status()
            print("Respuesta del servidor recibida (HTTP 200).")

            cookies_obtenidas = session.cookies.get_dict()

            if "WebFlowSessionId" in cookies_obtenidas:
                print("\n==============================================")
                print("✅ ¡ÉXITO! Login correcto.")
                print(f"Cookie de sesión obtenida: ...{cookies_obtenidas['WebFlowSessionId'][-6:]}")
                print("==============================================")

                # ¡IMPORTANTE! Esto es lo que pasa la cookie a XCom
                return cookies_obtenidas
            else:
                print("\n==============================================")
                print("❌ ERROR: Login fallido (No se obtuvo 'WebFlowSessionId').")
                print("==============================================")
                print("Causas probables: USERNAME o CLAVE incorrectos.")
                print("Respuesta del servidor (HTML):")
                print(resp_login.text[:500] + "...")
                raise ValueError("Login fallido. El servidor no devolvió 'WebFlowSessionId'. Revisa las credenciales.")

    except requests.exceptions.HTTPError as e:
        print(f"\n❌ ERROR HTTP: {e}")
        print(f"La 'WEBFLOW_LOGIN_URL' ({WEBFLOW_LOGIN_URL}) es incorrecta o el servidor falló.")
        raise
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR DE RED: {e}")
        print("No se pudo conectar al servidor.")
        raise
    except Exception as e:
        print(f"\n❌ ERROR INESPERADO: {e}")
        raise


# =================================================================
# --- FUNCIÓN ACTUALIZADA CON LÓGICA DE PAGOS ---
# =================================================================
def procesar_y_enriquecer_archivo_maestro(**kwargs):
    print("--- ⚙️ Iniciando procesamiento y enriquecimiento completo del archivo maestro ---")

    # --- INICIO DE MODIFICACIÓN: OBTENER COOKIES DE XCOM ---
    print("--- 🔐 Obteniendo cookies de sesión de WebFlow desde XCom ---")
    ti = kwargs['ti']
    cookie_dict = ti.xcom_pull(task_ids='t_login_webflow')

    if not cookie_dict or 'WebFlowSessionId' not in cookie_dict:
        raise ValueError("No se pudieron obtener las cookies de WebFlow del XCom. La tarea de login falló.")

    print(f"Cookies de sesión obtenidas exitosamente. SessionId: ...{cookie_dict['WebFlowSessionId'][-6:]}")

    # --- FIN DE MODIFICACIÓN ---

    # --- INICIO DE MODIFICACIÓN (Función de ajuste de montos) ---
    def ajustar_monto_pago(monto):
        # La columna ya habrá sido convertida a numérico, pero volvemos a chequear NaT/NaN
        if pd.isna(monto):
            return monto

        if monto >= 10000000:
            return monto / 100
        elif monto >= 1000:
            return monto / 10
        else:
            # No se divide (cubre >= 100 y < 100)
            return monto

    # --- FIN DE MODIFICACIÓN (Función de ajuste de montos) ---

    resumen_gestiones = pd.DataFrame()
    df_estado_total = pd.DataFrame()

    try:
        print("Obteniendo datos de gestiones...")
        gestiones_cookies = cookie_dict
        gestiones_headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded",
                             "Referer": "https://cloud.webflow.com.ar/transvalores/cgi-bin/rephistoriales.cgi"}
        data_gestiones = {"Op": 4, "Nivel": 0, "IdControl": "", "IdRegistro": "", "UTC": 3936354129, "ID_CLIENTE": 0,
                          "ID_ENTREGA": 0, "ID_CAMPANIA": 0, "FECHA_INICIAL": "01/01/2024", "HORA_INICIAL": "00:00",
                          "FECHA_FINAL": datetime.now().strftime('%d/%m/%Y'), "HORA_FINAL": "23:59", "GRUPO": "TODOS",
                          "REL_ESTIMULO": "<>",
                          "ID_ESTIMULO": 186, "REL_USUARIO": "<>", "ID_USUARIO": 1, "SUMA_ACTUAL": "Actual",
                          "SUMA_ORIGINAL": "Original", "incluir_entrega": 1, "incluir_cliente": 1, "incluir_costo": 1}

        resp_gestiones = requests.post("https://cloud.webflow.com.ar/transvalores/cgi-bin/rephistoriales.cgi",
                                       headers=gestiones_headers, cookies=gestiones_cookies, data=data_gestiones,
                                       timeout=500)
        resp_gestiones.raise_for_status()
        df_gestiones = pd.read_csv(BytesIO(resp_gestiones.content), sep="\t", encoding="latin1")

        if 'NRO DOC' in df_gestiones.columns and 'FECHA' in df_gestiones.columns:
            df_gestiones["NRO DOC"] = df_gestiones["NRO DOC"].astype(str).str.extract(r"(\d+)").astype(float).astype(
                "Int64")
            df_gestiones["FECHA"] = pd.to_datetime(df_gestiones["FECHA"], dayfirst=True, errors="coerce")
            resumen_gestiones = df_gestiones.groupby("NRO DOC").agg(TOTAL_GESTIONES=("NRO DOC", "count"),
                                                                    ULTIMA_GESTION=("FECHA", "max")).reset_index()
            print("✅ Datos de gestiones obtenidos y validados.")
    except Exception as e:
        print(f"❌ ERROR al obtener gestiones: {e}. Se continuará sin estos datos.")

    print("Obteniendo datos de estado de negociación (nueva versión)...")
    ids_clientes = [260, 333, 330, 329, 328, 325, 324, 322, 313, 334]

    def limpiar_illegales(texto):
        if pd.isna(texto): return texto
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", str(texto))

    def descargar_reporte_estado(id_cliente: int, session_cookies: dict):
        print(f"  - Intentando descargar reporte para ID_CLIENTE: {id_cliente}")

        estados_seleccionados = [
            302, 303, 305, 337, 306, 330, 334, 300, 316, 333, 332,
            331, 339, 321, 293, 294, 324, 315, 417, 338, 323, 319,
            329, 314, 290, 322, 301, 320, 326, 308, 309
        ]

        max_retries = 2
        for intento in range(max_retries):
            try:
                estado_headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "es-419,es;q=0.9,en;q=0.8",
                    "Connection": "keep-alive",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://cloud.webflow.com.ar",
                    "Referer": "https://cloud.webflow.com.ar/transvalores/cgi-bin/reportes.cgi",
                    "User-Agent": USER_AGENT,
                }

                with requests.Session() as s:
                    s.cookies.update(session_cookies)

                    # 2. Construimos el payload como lista de tuplas para permitir duplicados
                    payload_base = [
                        ("ID_LISTADO", "77"),
                        ("IDS_CLIENTE", str(id_cliente)),
                        ("IDS_CLIENTE__FMT", ",%d"),
                        ("LEGAJO_DESDE", "0"),
                        ("LEGAJO_HASTA", "9999999"),
                        ("ESTADOS__FMT", ",%d")
                    ]

                    # 3. Agregamos dinámicamente cada estado a la lista
                    for estado in estados_seleccionados:
                        payload_base.append(("ESTADOS", str(estado)))

                    # Llamada 1: OP 102 (Notificación)
                    p102 = payload_base.copy()
                    p102.append(("OP", "102"))

                    s.post("https://cloud.webflow.com.ar/transvalores/cgi-bin/reportes.cgi",
                           headers=estado_headers, data=p102, timeout=60)

                    # Llamada 2: Op 4 (Descarga pesada)
                    p4 = payload_base.copy()
                    p4.append(("Op", "4"))
                    p4.append(("UTC", "-356884542"))

                    resp = s.post("https://cloud.webflow.com.ar/transvalores/cgi-bin/reportes.cgi",
                                  headers=estado_headers, data=p4, timeout=1200)  # Subí a 1200 por las dudas

                    if resp.status_code == 200 and resp.content.strip():
                        if resp.content.strip().startswith(b'<'):
                            print(
                                f"  - ⚠️ Respuesta HTML (posible error de sesión) en ID {id_cliente}. Reintentando...")
                            continue

                        df = pd.read_csv(BytesIO(resp.content), sep="\t", encoding="latin1", on_bad_lines="skip",
                                         engine='python')
                        df["ID_CLIENTE"] = id_cliente
                        df = df.map(limpiar_illegales)
                        print(f"  - ✅ ÉXITO: Descargado ID_CLIENTE {id_cliente} ({len(df)} filas)")
                        return df

                    elif resp.status_code == 502:
                        print(f"  - ⚠️ Error 502 en ID {id_cliente}. Intento {intento + 1}/{max_retries}")
                        if intento < max_retries - 1:
                            import time
                            time.sleep(20)  # Más tiempo de espera para que el server se recupere
                    else:
                        print(f"  - ⚠️ Status {resp.status_code} para ID {id_cliente}")

            except Exception as e:
                print(f"  - ❌ ERROR en ID {id_cliente} (intento {intento + 1}): {e}")

        return pd.DataFrame()

    lista_dfs_estado = [descargar_reporte_estado(cliente, cookie_dict) for cliente in ids_clientes]
    df_estado_total = pd.concat([df for df in lista_dfs_estado if not df.empty], ignore_index=True)

    if not df_estado_total.empty:
        df_estado_total.columns = [c.strip().upper() for c in df_estado_total.columns]
        columna_dni_encontrada = next((nombre for nombre in ['DNI', 'NRO DOC', 'DOCUMENTO', 'NRO. DOCUMENTO'] if
                                       nombre in df_estado_total.columns), None)
        if columna_dni_encontrada:
            df_estado_total.rename(columns={columna_dni_encontrada: 'DNI'}, inplace=True)
            df_estado_total["DNI"] = pd.to_numeric(df_estado_total["DNI"], errors="coerce").astype("Int64")

            # ==================================================================
            # --- MODIFICACIÓN 1: LIMPIEZA DE FECHAS Y MONTOS DE PAGO ---
            # ==================================================================

            # Limpiar FECHA ULT PAGO
            if 'FECHA ULT PAGO' in df_estado_total.columns:
                print("  - Procesando 'FECHA ULT PAGO'...")
                df_estado_total['FECHA ULT PAGO'] = pd.to_datetime(df_estado_total['FECHA ULT PAGO'], dayfirst=True,
                                                                   errors='coerce')
                # Definir la fecha incorrecta que queremos filtrar
                fecha_mala = pd.to_datetime('1990-01-01')
                # Reemplazar la fecha incorrecta por NaT (Not a Time)
                df_estado_total.loc[df_estado_total['FECHA ULT PAGO'] == fecha_mala, 'FECHA ULT PAGO'] = pd.NaT
                print("  - Columna 'FECHA ULT PAGO' procesada y fechas '1/1/1990' eliminadas.")

            # Limpiar ULT PAGO (monto)
            if 'ULT PAGO' in df_estado_total.columns:
                print("  - Procesando 'ULT PAGO' (monto)...")
                df_estado_total['ULT PAGO'] = (
                    df_estado_total['ULT PAGO']
                    .astype(str)
                    .str.replace(r'[$.]', '', regex=True)  # Quitar $ y punto (miles)
                    .str.replace(',', '.', regex=False)  # Cambiar coma (decimal) por punto
                    .str.strip()
                )
                df_estado_total['ULT PAGO'] = pd.to_numeric(df_estado_total['ULT PAGO'], errors='coerce')
                print("  - Columna 'ULT PAGO' procesada y convertida a numérico.")

                # --- INICIO DE MODIFICACIÓN (Aplicar ajuste de montos) ---
                print("  - Aplicando lógica de ajuste de montos (división por 10 o 100)...")
                df_estado_total['ULT PAGO'] = df_estado_total['ULT PAGO'].apply(ajustar_monto_pago)
                print("  - Lógica de ajuste de montos aplicada.")
                # --- FIN DE MODIFICACIÓN (Aplicar ajuste de montos) ---

            # ==================================================================
            # --- FIN MODIFICACIÓN 1 ---
            # ==================================================================

            print("✅ Datos de estado (nueva versión) obtenidos y consolidados.")

        else:
            df_estado_total = pd.DataFrame()

    print("\n--- ⚙️ Iniciando PASO 2: Lectura y procesamiento del archivo maestro ---")
    xls = pd.ExcelFile(EXCEL_MAESTRO_PATH)
    dataframes_por_hoja = {sheet_name: pd.read_excel(xls, sheet_name=sheet_name) for sheet_name in xls.sheet_names}
    print("Lectura completa del archivo maestro.")

    print("\n--- Procesando y enriqueciendo cada hoja en memoria... ---")
    for sheet_name, df in dataframes_por_hoja.items():
        if sheet_name == "Analisis": continue
        print(f"\n➡️  Aplicando enriquecimientos para la hoja: '{sheet_name}'")
        df.columns = [str(c).strip().upper() for c in df.columns]
        if "DNI" not in df.columns:
            print(f"  - Se omite enriquecimiento para esta hoja (no contiene columna 'DNI').")
            continue

        df["DNI"] = pd.to_numeric(df["DNI"], errors="coerce").astype("Int64")
        df.drop_duplicates(subset=['DNI'], keep='first', inplace=True)

        if not resumen_gestiones.empty:
            df.drop(columns=["ULTIMA_GESTION", "TOTAL_GESTIONES"], errors='ignore', inplace=True)
            df = df.merge(resumen_gestiones, how="left", left_on="DNI", right_on="NRO DOC").drop(columns=["NRO DOC"],
                                                                                                 errors='ignore')
            print("  - Merge de Gestiones: OK")

        # ==================================================================
        # --- MODIFICACIÓN 2: MERGE DINÁMICO DE DATOS DE WEBFLOW ---
        # ==================================================================
        if not df_estado_total.empty:
            # Definir qué columnas queremos traer de df_estado_total
            columnas_webflow = ['DNI']
            columnas_a_borrar_local = []

            # Chequear dinámicamente qué columnas existen para agregar al merge
            for col in ["ESTADO NEGOCIACION", "FECHA ULT PAGO", "ULT PAGO", "EMAIL"]:
                if col in df_estado_total.columns:
                    columnas_webflow.append(col)
                    columnas_a_borrar_local.append(col)

            if len(columnas_webflow) > 1:  # Solo si encontramos columnas además de DNI
                # Borrar las columnas viejas del df local para evitar duplicados
                df.drop(columns=columnas_a_borrar_local, errors='ignore', inplace=True)

                # Hacer el merge con las columnas seleccionadas
                df = df.merge(
                    df_estado_total[columnas_webflow].drop_duplicates(subset=['DNI']),
                    how="left",
                    on="DNI"
                )
                print(f"  - Merge de WebFlow ({', '.join(columnas_a_borrar_local)}): OK")
        # ==================================================================
        # --- FIN MODIFICACIÓN 2 ---
        # ==================================================================

        debt_col = next((col for col in ['DEUDA TOTAL DEL CLIENTE', 'DEUDA_TOTAL', 'CAPITAL'] if col in df.columns),
                        None)
        if debt_col:
            def clasificar_deuda(deuda):
                deuda = pd.to_numeric(deuda, errors='coerce')
                if pd.isna(deuda): return "Sin Deuda"
                if deuda <= 150000: return "Bajo"
                if 150000 < deuda <= 450000: return "Medio"
                if 450000 < deuda <= 2000000: return "Medio Alto"
                return "Alto"

            df['TRAMO DEUDA'] = df[debt_col].apply(clasificar_deuda)
            print(f"  - Cálculo de TRAMO DEUDA (usando '{debt_col}'): OK")

        if 'ULTIMA_GESTION' in df.columns:
            ultima_gestion_dt = pd.to_datetime(df['ULTIMA_GESTION'], errors='coerce')
            df['ULTIMA_GESTION'] = ultima_gestion_dt.dt.strftime('%Y-%m-%d')
            # Reemplazar 'NaT' (texto) por un valor nulo real
            df['ULTIMA_GESTION'] = df['ULTIMA_GESTION'].replace('NaT', np.nan)

            dias_diff = (datetime.now() - ultima_gestion_dt).dt.days
            df['DIAS_SIN_GESTIONAR'] = np.floor(pd.to_numeric(dias_diff, errors='coerce')).astype('Int64')
            print("  - Cálculo de DIAS_SIN_GESTIONAR: OK")

        # ==================================================================
        # --- MODIFICACIÓN 2.5: FORMATEO DE FECHA ULT PAGO ---
        # ==================================================================
        if 'FECHA ULT PAGO' in df.columns:
            # La columna ya debería ser datetime por el merge, solo formatear a texto
            fecha_pago_dt = pd.to_datetime(df['FECHA ULT PAGO'], errors='coerce')
            df['FECHA ULT PAGO'] = fecha_pago_dt.dt.strftime('%Y-%m-%d')
            # Reemplazar 'NaT' (texto) por un valor nulo real
            df['FECHA ULT PAGO'] = df['FECHA ULT PAGO'].replace('NaT', np.nan)
            print("  - Formato de FECHA ULT PAGO a YYYY-MM-DD: OK")
        # ============================================================
        # FILTRAR FILAS SIN ESTADO NEGOCIACION
        # ============================================================
        if 'ESTADO NEGOCIACION' in df.columns:
            filas_antes = len(df)

            df = df[
                df['ESTADO NEGOCIACION'].notna() &
                (df['ESTADO NEGOCIACION'].astype(str).str.strip() != "")
                ]

            filas_despues = len(df)

            print(f"  - Filtrado sin ESTADO NEGOCIACION: {filas_antes - filas_despues} filas eliminadas")
        # ============================================================
        # ==================================================================
        # --- FIN MODIFICACIÓN 2.5 ---
        # ==================================================================

        dataframes_por_hoja[sheet_name] = df

    # ==================================================================
    # --- MODIFICACIÓN 3: ACTUALIZACIÓN DE HOJA "CONSOLIDADO" ---
    # ==================================================================
    print("\n--- ⚙️ Generando Hoja 'Consolidado' ---")
    lista_consolidados = []
    # Definir columnas deseadas para el consolidado en el orden deseado
    columnas_consolidadas_deseadas = ['DNI', 'ESTADO NEGOCIACION', 'FECHA ULT PAGO', 'ULT PAGO', 'NOMBREHOJA', 'EMAIL']

    for sheet_name, df in dataframes_por_hoja.items():
        if sheet_name in ["Analisis", "Consolidado"]:
            continue
        if "DNI" not in df.columns:
            print(f"  - Omitiendo hoja '{sheet_name}' (sin DNI) para consolidado.")
            continue

        temp_df = pd.DataFrame()
        temp_df['DNI'] = df['DNI']
        temp_df['NOMBREHOJA'] = sheet_name

        # Asignar dinámicamente solo las columnas que existen en la hoja 'df'
        for col in ['ESTADO NEGOCIACION', 'FECHA ULT PAGO', 'ULT PAGO', 'EMAIL']:
            if col in df.columns:
                temp_df[col] = df[col]
            else:
                temp_df[col] = np.nan  # Asegurarse que la columna exista para el concat

        lista_consolidados.append(temp_df)

    if lista_consolidados:
        df_consolidado = pd.concat(lista_consolidados, ignore_index=True)

        # Reordenar y filtrar columnas finales basado en las que realmente existen
        columnas_finales_existentes = [col for col in columnas_consolidadas_deseadas if col in df_consolidado.columns]
        df_consolidado = df_consolidado[columnas_finales_existentes]

        dataframes_por_hoja["Consolidado"] = df_consolidado
        print(f"--- ✅ Hoja 'Consolidado' generada con columnas: {', '.join(columnas_finales_existentes)} ---")
    else:
        print("--- ⚠️ No se generó la hoja 'Consolidado' (no se encontraron datos en las hojas). ---")
    # ==================================================================
    # --- FIN MODIFICACIÓN 3 ---
    # ==================================================================

    print("\n--- Escribiendo el archivo maestro final... ---")
    with pd.ExcelWriter(EXCEL_MAESTRO_PATH, engine='openpyxl') as writer:
        for sheet_name, df in dataframes_por_hoja.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print("--- ✅ Procesamiento y enriquecimiento completo finalizado. ---")


def subir_archivo_modificado():
    print("--- 📤 Tarea Final: Subiendo archivo modificado a Drive ---")
    upload_file(EXCEL_MAESTRO_ID, EXCEL_MAESTRO_PATH)


# =================================================================
# DEFINICIÓN DEL DAG
# =================================================================
with DAG(
        dag_id="dag_etl_multifuente",
        schedule=None,
        start_date=timezone.utcnow() - timedelta(days=1),
        catchup=False,
        tags=["etl", "multifuente", "optimizado"],
) as dag:
    t1_descargar = PythonOperator(task_id="descargar_archivos_excel", python_callable=descargar_archivos_iniciales)

    with TaskGroup("procesamiento_de_fuentes") as tg1_procesamiento:
        for fuente in FUENTES_DE_DATOS:
            task_id_limpio = fuente['nombre'].replace(" ", "_")
            PythonOperator(
                task_id=f"procesar_{task_id_limpio}",
                python_callable=procesar_fuente,
                op_kwargs={"config": fuente}
            )

    t_consolidar = PythonOperator(task_id="consolidar_resultados_en_excel", python_callable=consolidar_resultados)

    t_login_webflow = PythonOperator(
        task_id="t_login_webflow",
        python_callable=task_login_y_obtener_cookie
    )

    t_procesamiento_final = PythonOperator(
        task_id="procesar_y_enriquecer_maestro",
        python_callable=procesar_y_enriquecer_archivo_maestro
    )

    t_subir = PythonOperator(task_id="subir_archivo_final", python_callable=subir_archivo_modificado)

    # Definición de dependencias
    t1_descargar >> tg1_procesamiento >> t_consolidar >> t_login_webflow >> t_procesamiento_final >> t_subir