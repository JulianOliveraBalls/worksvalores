import requests
import pandas as pd
from io import StringIO
import time
import logging

class WebFlowAPI:
    def __init__(self, username="julian.olivera", password="Julian2016"):
        self.url_base = "https://cloud.webflow.com.ar/transvalores/cgi-bin"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        self.username = username
        self.password = password

    def login(self):
        logging.info(f"🔑 Iniciando sesión para: {self.username}")
        payload = {'USERNAME': self.username, 'CLAVE': self.password, 'COUNT': '', 'Op': '', 'homePath': ''}
        try:
            res = self.session.post(f"{self.url_base}/log.cgi", data=payload, headers=self.headers)
            if res.status_code == 200 and "ERROR" not in res.text.upper():
                logging.info("✅ Login exitoso.")
                return True
            return False
        except Exception as e:
            logging.error(f"❌ Error en conexión de login: {e}")
            return False

    def _fetch(self, endpoint, payload, name, needs_prep=False):
        logging.info(f"🚀 Procesando reporte: {name}")
        
        if needs_prep:
            prep_payload = payload.copy()
            prep_payload['OP'] = '102'
            if 'Op' in prep_payload: del prep_payload['Op']
            logging.info(f"  > Enviando Paso 102 (Preparación) para {name}")
            self.session.post(f"{self.url_base}/{endpoint}", data=prep_payload, headers=self.headers)
            time.sleep(0.5)

        res = self.session.post(f"{self.url_base}/{endpoint}", data=payload, headers=self.headers)
        
        if res.status_code == 200 and len(res.content) > 100 and b"<HTML" not in res.content[:100].upper():
            df = pd.read_csv(StringIO(res.text), sep='\t', encoding='latin-1', on_bad_lines='skip')
            logging.info(f"  📊 {name}: {len(df)} filas.")
            return df
        
        logging.warning(f"  ⚠️ {name}: No se obtuvieron datos o el servidor devolvió HTML.")
        return pd.DataFrame()

    def get_gestiones(self, f_inicio, f_fin):
        payload = {
            "Op": "4", "UTC": "-341767742", "ID_CLIENTE": "0", "FECHA_INICIAL": f_inicio, "FECHA_FINAL": f_fin,
            "HORA_INICIAL": "00:00", "HORA_FINAL": "23:59", "ID_ESTIMULO": "186", "ID_USUARIO": "1", "GRUPO": "TODOS",
            "REL_ESTIMULO": "<>", "REL_USUARIO": "<>", "incluir_entrega": "1", "incluir_cliente": "1", "incluir_costo": "1"
        }
        return self._fetch("rephistoriales.cgi", payload, "Gestiones")

    def get_llamados(self, f_inicio, f_fin):
        payload = {
            "Op": "4", "UTC": "-341769448", "FECHA_DESDE": f_inicio, "HORA_DESDE": "0", 
            "FECHA_HASTA": f_fin, "HORA_HASTA": "0", "INTERNO": "-1", "TIPO": "1", "OPERADOR": "-1"
        }
        return self._fetch("replistadollamados.cgi", payload, "Llamados")

    def get_perdidas(self, f_inicio, f_fin):
        payload = {"Op": "4", "UTC": "-341769448", "FECHA_DESDE": f_inicio, "FECHA_HASTA": f_fin}
        return self._fetch("repllamadasperdidas.cgi", payload, "Perdidas")

    def get_aviso_pago(self, f_inicio, f_fin):
        payload = {
            "Op": "4", "Nivel": "0", "UTC": "-341767724", "ID_LISTADO": "23", 
            "IDS_OPERADOR": "0", "IDS_OPERADOR__FMT": ",%d", "FECHA_DESDE": f_inicio, "FECHA_HASTA": f_fin
        }
        return self._fetch("reportes.cgi", payload, "Aviso de Pago", needs_prep=True)

    def get_entregas(self, f_inicio, f_fin):
        payload = {
            "Op": "4", "Nivel": "0", "UTC": "-341769323", "ID_LISTADO": "99",
            "IDS_ENTREGA": "0-0", "DESDE": f_inicio, "HASTA": f_fin
        }
        return self._fetch("reportes.cgi", payload, "Entregas", needs_prep=True)