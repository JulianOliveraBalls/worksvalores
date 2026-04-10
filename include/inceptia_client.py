import requests
import pandas as pd
import time
import zipfile
import io
import logging
import os

class InceptiaAPI:
    """
    Cliente avanzado para la API de Inceptia.
    
    Implementa gestión de sesiones persistentes, autenticación JWT con auto-renovación,
    manejo de Rate Limits (429) y procesamiento de archivos masivos en memoria.
    """

    def __init__(self, email=None, password=None, base_url=None):
        """
        Inicializa la clase cargando credenciales desde el entorno.
        Establece una sesión de requests para optimizar la reutilización de conexiones TCP.
        """
        self.email = email or os.getenv("INCEPTIA_EMAIL")
        self.password = password or os.getenv("INCEPTIA_PASSWORD")
        self.base_url = base_url or os.getenv("INCEPTIA_BASE_URL")

        if not self.email or not self.password:
            raise ValueError("Error: Faltan credenciales obligatorias en el entorno.")

        self.session = requests.Session()
        self.access_token = None

        # Headers globales: Identificación del cliente y tipo de contenido
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (DataOps-Transvalores-Service)",
        })

    def login(self):
        """
        Obtiene un token JWT. Actualiza los headers de la sesión para peticiones subsiguientes.
        Soporta los esquemas de respuesta 'token' y 'access'.
        """
        url = f"{self.base_url}/auth/login/"
        payload = {"email": self.email, "password": self.password}

        # Timeout preventivo para evitar que el login bloquee el thread de Airflow
        res = self.session.post(url, json=payload, timeout=30)

        if res.status_code != 200:
            logging.error(f"Fallo de Login {res.status_code}: {res.text}")
            raise Exception("Credenciales de Inceptia inválidas o API caída.")

        data = res.json()
        self.access_token = data.get('token') or data.get('access')

        if not self.access_token:
            raise Exception("No se encontró un token válido en la respuesta de autenticación.")

        self.session.headers["Authorization"] = f"JWT {self.access_token}"
        logging.info("🔐 Autenticación JWT exitosa y token actualizado.")

    def _request(self, method, url, **kwargs):
        """
        Middleware central de peticiones. 
        Maneja:
        - 401 (Unauthorized): Lanza re-login automático.
        - 429 (Too Many Requests): Pausa la ejecución por 30s.
        - 5xx (Server Error): Reintento exponencial simple.
        """
        if "timeout" not in kwargs:
            kwargs["timeout"] = 45 # Timeout robusto para reportes pesados

        for intento in range(1, 6):
            res = self.session.request(method, url, **kwargs)

            if res.status_code in [200, 201]:
                return res

            if res.status_code == 401:
                logging.warning(f"⚠️ Token expirado. Intento de re-login {intento}/5...")
                self.login()
                continue

            if res.status_code == 429:
                logging.warning(f"🚫 Rate Limit alcanzado. Esperando 30s para reintentar...")
                time.sleep(30)
                continue

            if res.status_code >= 500:
                logging.warning(f"🔥 Error de servidor {res.status_code}. Reintento {intento}...")
                time.sleep(10)
                continue

            raise Exception(f"Error crítico de API {res.status_code}: {res.text}")

        raise Exception("Max retries alcanzado. La API no responde correctamente.")

    def obtener_reporte_zip(self, bot_id, fecha_inicio, fecha_fin):
        """
        Extrae datos mediante el endpoint de tareas asincrónicas (Bulk Export).
        Es el método recomendado para volúmenes altos de datos (>500 registros).
        
        Lógica:
        1. Crea la tarea (Task).
        2. Realiza Polling (consulta estado) cada 10s.
        3. Descarga y procesa el ZIP en memoria (BytesIO) sin escribir en disco.
        """
        if not self.access_token:
            self.login()

        payload = {
            "type": "EXPORT_CAMPAIGN",
            "bot_key": int(bot_id),
            "filter": {
                "campaign__bot": int(bot_id),
                "case_result__name__in": None,
                "last_updated__date__gte": fecha_inicio,
                "last_updated__date__lte": fecha_fin
            },
            "task_params": {"bot_id": int(bot_id)},
            "search_fields": ["code", "phone"],
            "search": ""
        }

        logging.info(f"📦 Solicitando exportación masiva para Bot {bot_id}...")
        res_task = self._request("POST", f"{self.base_url}/api/v2/tasks/", json=payload)
        task_id = res_task.json().get('id')

        file_url = None
        # Máximo 30 intentos (5 minutos de espera total)
        for _ in range(30):
            res_status = self._request("GET", f"{self.base_url}/api/v2/tasks/{task_id}/")
            data = res_status.json()
            status = data.get("status")

            logging.info(f"⏳ Tarea {task_id} estado: {status}")

            if status in ["SUCCESS", "COMPLETED", "DONE"]:
                result = data.get("result", {})
                
                # Manejo de casos donde la tarea termina pero no hay gestiones
                if isinstance(result, dict) and result.get("total_rows") == 0:
                    logging.warning("La tarea finalizó pero no se encontraron registros.")
                    return pd.DataFrame()
                
                file_url = result.get("file") if isinstance(result, dict) else data.get("file")
                break
            
            time.sleep(10)

        if not file_url:
            logging.error("No se pudo obtener el link de descarga tras el polling.")
            return pd.DataFrame()

        # Descarga y lectura de CSV con manejo de BOM (utf-8-sig)
        full_url = file_url if file_url.startswith("http") else f"{self.base_url}{file_url}"
        res_file = self._request("GET", full_url)

        with zipfile.ZipFile(io.BytesIO(res_file.content)) as z:
            nombre_csv = z.namelist()[0]
            return pd.read_csv(z.open(nombre_csv), sep=None, engine="python", encoding="utf-8-sig")

    def obtener_todos_los_casos(self, bot_id, fecha_inicio, fecha_fin, estado=None):
        """
        Extrae datos mediante el endpoint de campañas con paginación (Next Link).
        Ideal para consultas rápidas o filtrado específico desde el servidor.
        
        Args:
            estado (str): Si se provee, filtra los resultados por 'result' en la API.
        """
        if not self.access_token:
            self.login()

        url = f"{self.base_url}/api/v2/campaigns/cases/{bot_id}/"
        
        params = {
            "local_created__gte": fecha_inicio,
            "local_created__lte": fecha_fin,
            "page": 1,
            "page_size": 100
        }

        if estado:
            params["result"] = estado
            logging.info(f"🔍 Filtrando por servidor: {estado}")
        else:
            logging.info("🌐 Extrayendo universo completo de casos (Paginado).")

        todos_los_casos = []

        while url:
            # Pasa params solo en la primera llamada; luego el Next Link ya los incluye
            res = self._request("GET", url, params=params if params.get("page") else None)
            data = res.json()

            results = data.get("results", [])
            todos_los_casos.extend(results)

            logging.info(f"📑 Casos acumulados: {len(todos_los_casos)}")

            url = data.get("next")
            params = {} # Reset para evitar conflictos en la navegación por puntero
            
            if url:
                time.sleep(1.5) # Throttling preventivo para evitar 429

        if not todos_los_casos:
            return pd.DataFrame()

        # json_normalize aplana las estructuras anidadas (ej: case_result.name)
        df = pd.json_normalize(todos_los_casos)
        logging.info(f"✅ Extracción finalizada. Total: {len(df)} registros.")

        return df