import requests
import pandas as pd
from io import StringIO
import time
import logging

class WebFlowAPI:
    def __init__(self):
        self.url_base = "https://cloud.webflow.com.ar/transvalores/cgi-bin"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        self.username = "julian.olivera"
        self.password = "Julian2016"
        self._utcs = {
            "gestiones": "-341768792",
            "entregas": "-341489276", "llamados": "-341769448",
            "perdidas": "-341493676", "convenios_99": "-341488551",
            "aviso_pago": "-341168283"
        }

    def login(self):
        payload = {'USERNAME': self.username, 'CLAVE': self.password, 'Op': '', 'homePath': ''}
        res = self.session.post(f"{self.url_base}/log.cgi", data=payload, headers=self.headers)
        return res.status_code == 200 and "ERROR" not in res.text.upper()

    def _fetch(self, endpoint, payload, name, prep_op=None):
        if prep_op:
            prep_data = list(payload) if isinstance(payload, list) else payload.copy()
            if isinstance(prep_data, list):
                prep_data = [(k, v) if k.upper() != 'OP' else ('OP', str(prep_op)) for k, v in prep_data]
            else:
                prep_data['OP'] = str(prep_op)
            self.session.post(f"{self.url_base}/{endpoint}", data=prep_data, headers=self.headers)
            time.sleep(2)
        res = self.session.post(f"{self.url_base}/{endpoint}", data=payload, headers=self.headers)
        if res.status_code == 200 and len(res.text) > 50 and "<HTML" not in res.text.upper()[:100]:
            return pd.read_csv(StringIO(res.text), sep='\t', encoding='latin-1', on_bad_lines='skip', quoting=3)
        return pd.DataFrame()

    def get_entregas(self, ids_cliente=[330, 260], estados=[302, 303]):
        payload = [
            ("Op", "4"), ("Nivel", "0"), ("IdControl", ""), ("IdRegistro", ""),
            ("UTC", self._utcs["entregas"]), ("ID_LISTADO", "77"),
            ("IDS_CLIENTE__FMT", ",%d"), ("LEGAJO_DESDE", "0"), ("LEGAJO_HASTA", "9999999"),
            ("ESTADOS__FMT", ",%d")
        ]
        for cid in ids_cliente: payload.append(("IDS_CLIENTE", str(cid)))
        for est in estados: payload.append(("ESTADOS", str(est)))
        return self._fetch("reportes.cgi", payload, "Entregas", prep_op=102)

    def get_gestiones(self, f_desde, f_hasta, id_cliente=0, id_estimulo=0, id_usuario=0, rel_estimulo="=", rel_usuario="="):
        payload = {
            "Op": 4, "UTC": self._utcs["gestiones"], "ID_CLIENTE": id_cliente,
            "FECHA_INICIAL": f_desde, "FECHA_FINAL": f_hasta, "HORA_INICIAL": "00:00", "HORA_FINAL": "23:59",
            "ID_ESTIMULO": id_estimulo, "ID_USUARIO": id_usuario, "GRUPO": "TODOS",
            "REL_ESTIMULO": rel_estimulo, "REL_USUARIO": rel_usuario, "incluir_entrega": 1, "incluir_cliente": 1, "incluir_costo": 1
        }
        return self._fetch("rephistoriales.cgi", payload, "Gestiones")
    
    def get_convenios_pago(self, f_desde, f_hasta):
        payload = {
            "Op": 4, "Nivel": 0, "IdControl": "", "IdRegistro": "",
            "UTC": self._utcs["convenios_99"], "ID_LISTADO": 99,
            "IDS_ENTREGA": "0-0", "DESDE": f_desde, "HASTA": f_hasta
        }
        return self._fetch("reportes.cgi", payload, "Convenios de Pago", prep_op=102)
    
    def get_aviso_pago(self, f_desde, f_hasta):
        payload = {
            "Op": 4, "Nivel": 0, "IdControl": "", "IdRegistro": "", 
            "UTC": self._utcs["aviso_pago"],
            "ID_CLIENTE": 0, "FECHA_DESDE": f_desde, "ID_ENTREGA": 0, 
            "FECHA_HASTA": f_hasta, "ID_USUARIO": 0, "GRUPO": "TODOS"
        }
        
        return self._fetch("reppromesas.cgi", payload, "Avisos de Pago", prep_op=102)
    
    def get_perdidas(self, f_desde, f_hasta):
        payload = { "Op": 4, "Nivel": 0, "UTC": self._utcs["perdidas"], "FECHA_DESDE": f_desde, "FECHA_HASTA": f_hasta }
        return self._fetch("repllamadasperdidas.cgi", payload, "Perdidas", prep_op=3)

    def get_llamados(self, f_desde, f_hasta):
        payload = {
            "Op": 4, "UTC": self._utcs["llamados"], "FECHA_DESDE": f_desde, "HORA_DESDE": "0", 
            "FECHA_HASTA": f_hasta, "HORA_HASTA": "0", "INTERNO": -1, "TIPO": 1, "OPERADOR": -1
        }
        return self._fetch("replistadollamados.cgi", payload, "Llamados")