import os
import sys
from datetime import datetime

# 1. ESTO TIENE QUE SER LO PRIMERO QUE LEA EL SCRIPT
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/home/airflow/.cache/ms-playwright"

# 2. RECIÉN AHORA IMPORTAMOS PLAYWRIGHT
from playwright.sync_api import sync_playwright
from slack_sdk import WebClient

# Configuración de rutas
BASE_DIR = "/opt/airflow/include"
USER_DATA_DIR = "/opt/airflow/include/user_data"
FECHA = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"reporte_cashea_{FECHA}.csv")

def run():
    print(f"🚀 Iniciando bot. Buscando navegadores en: {os.environ['PLAYWRIGHT_BROWSERS_PATH']}")
    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=USER_DATA_DIR,
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )
            page = context.new_page()
            
            print("🌐 Navegando a Retool...")
            page.goto("https://support.cashea.retool.com/apps/dc2f2576-0cea-11f1-bd08-17e6dccfb9af/Asignaciones", timeout=90000)
            
            page.wait_for_selector("text=Descargar CSV", timeout=60000)
            
            print("📥 Iniciando descarga...")
            with page.expect_download(timeout=60000) as download_info:
                page.get_by_text("Descargar CSV").click()
            
            download = download_info.value
            download.save_as(OUTPUT_FILE)
            
            # Enviar a Slack
            token = os.getenv("SLACK_BOT_TOKEN")
            if not token:
                print("⚠️ Error: No hay SLACK_BOT_TOKEN")
                return

            client = WebClient(token=token)
            client.files_upload_v2(
                channel="C0ARL1VRSVC", 
                file=OUTPUT_FILE, 
                title=f"Reporte Cashea {FECHA}",
                initial_comment=f"✅ Reporte generado correctamente: {FECHA}"
            )
            print(f"🏁 Éxito total: {FECHA}")
            
        except Exception as e:
            print(f"❌ Error durante la ejecución: {e}")
            raise e
        finally:
            if 'context' in locals():
                context.close()

if __name__ == "__main__":
    run()