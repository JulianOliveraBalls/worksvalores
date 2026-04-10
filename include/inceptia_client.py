import requests
import pandas as pd
import time
import zipfile
import io
import logging
import os


class InceptiaAPI:
    def __init__(self, email=None, password=None, base_url=None):
        self.email = email or os.getenv("INCEPTIA_EMAIL")
        self.password = password or os.getenv("INCEPTIA_PASSWORD")
        self.base_url = base_url or os.getenv("INCEPTIA_BASE_URL")

        if not self.email or not self.password:
            raise ValueError("Faltan credenciales en variables de entorno")

        self.session = requests.Session()
        self.access_token = None

        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        })

    def login(self):
        url = f"{self.base_url}/auth/login/"
        payload = {"email": self.email, "password": self.password}

        res = self.session.post(url, json=payload)

        if res.status_code != 200:
            logging.error(f"Login error {res.status_code}: {res.text}")
            raise Exception("Error en login")

        data = res.json()
        self.access_token = data.get('token') or data.get('access')

        if not self.access_token:
            raise Exception("No se obtuvo token")

        self.session.headers["Authorization"] = f"JWT {self.access_token}"
        logging.info("Login exitoso")

    def _request(self, method, url, **kwargs):
        for _ in range(5):
            res = self.session.request(method, url, **kwargs)

            if res.status_code in [200, 201]:
                return res

            if res.status_code == 401:
                logging.warning("Token expirado → relogin")
                self.login()
                continue

            if res.status_code == 429:
                logging.warning("Rate limit → esperando 30s")
                time.sleep(30)
                continue

            if res.status_code >= 500:
                logging.warning(f"Server error {res.status_code}")
                time.sleep(10)
                continue

            raise Exception(f"Error {res.status_code}: {res.text}")

        raise Exception("Max retries alcanzado")

    def obtener_reporte_zip(self, bot_id, fecha):
        if not self.access_token:
            self.login()

        payload = {
            "type": "EXPORT_CAMPAIGN",
            "bot_key": str(bot_id),
            "filter": {
                "campaign__bot": bot_id,
                "last_updated__date__gte": fecha,
                "last_updated__date__lte": fecha
            },
            "task_params": {"bot_id": bot_id},
        }

        logging.info("Solicitando exportación...")
        res_task = self._request("POST", f"{self.base_url}/api/v2/tasks/", json=payload)
        task_id = res_task.json().get('id')

        file_url = None

        for _ in range(30):
            res_status = self._request("GET", f"{self.base_url}/api/v2/tasks/{task_id}/")
            data = res_status.json()
            status = data.get("status")

            logging.info(f"Tarea {task_id}: {status}")

            if status in ["SUCCESS", "COMPLETED", "DONE"]:
                result = data.get("result", {})
                file_url = result.get("file") if isinstance(result, dict) else data.get("file")
                break

            time.sleep(10)

        if not file_url:
            raise Exception("No se obtuvo archivo")

        full_url = file_url if file_url.startswith("http") else f"{self.base_url}{file_url}"
        logging.info(f"Descargando {full_url}")

        res_file = self._request("GET", full_url)

        with zipfile.ZipFile(io.BytesIO(res_file.content)) as z:
            archivo = z.namelist()[0]
            return pd.read_csv(z.open(archivo), sep=None, engine="python", encoding="latin-1")
        
    def obtener_todos_los_casos(self, bot_id, fecha_inicio, fecha_fin, resultado=None):
        if not self.access_token:
            self.login()

        url = f"{self.base_url}/api/v2/campaigns/cases/{bot_id}/"

        params = {
            "local_created__gte": fecha_inicio,
            "local_created__lte": fecha_fin,
            "page": 1
        }

        if resultado:
            params["result"] = resultado

        todos = []

        logging.info(f"Buscando casos desde {fecha_inicio} a {fecha_fin}")

        while url:
            res = self._request("GET", url, params=params if "page" in params else None)
            data = res.json()

            results = data.get("results", [])
            todos.extend(results)

            logging.info(f"Casos acumulados: {len(todos)}")

            url = data.get("next")
            params = {}
            time.sleep(0.5)

        if not todos:
            logging.warning("No se encontraron casos")
            return pd.DataFrame()

        df = pd.json_normalize(todos)
        logging.info(f"Total final: {len(df)} registros")

        return df