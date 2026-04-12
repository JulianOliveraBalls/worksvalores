import os
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright

def run_extraction():
    base_path = "/opt/airflow/include"
    state_path = f"{base_path}/state.json"
    
    file_asignaciones = f"{base_path}/reporte_asignaciones.csv"
    current_month = datetime.now().strftime("%Y-%m")
    file_pagos = f"{base_path}/reporte_pagos_{current_month}.csv"

    with sync_playwright() as p:
        print("🚀 Iniciando navegador...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        try:
            # --- PARTE 1: ASIGNACIONES ---
            print("📥 Extrayendo Asignaciones...")
            url_asig = "https://support.cashea.retool.com/apps/dc2f2576-0cea-11f1-bd08-17e6dccfb9af/Recuperaciones/Despachos%20-%20Asignaciones%20y%20Pagos/Asignaciones"
            page.goto(url_asig, wait_until="networkidle")
            
            # Esperamos específicamente el botón de asignaciones
            page.wait_for_selector('button[data-testid="Component::Action-AssignmentInstallmentsButton--0"]', timeout=30000)
            with page.expect_download() as download_info:
                page.get_by_test_id("Component::Action-AssignmentInstallmentsButton--0").click()
            
            download_asig = download_info.value
            download_asig.save_as(file_asignaciones)
            print(f"✅ Asignaciones guardadas.")

            # --- PARTE 2: PAGOS ---
            print("📥 Navegando a pestaña de Pagos...")
            url_pagos = "https://support.cashea.retool.com/apps/dc2f2576-0cea-11f1-bd08-17e6dccfb9af/Recuperaciones/Despachos%20-%20Asignaciones%20y%20Pagos/Pagos"
            page.goto(url_pagos, wait_until="networkidle")
            
            # USAMOS EL SELECTOR QUE ME PASASTE
            print("Esperando botón de descarga de Pagos...")
            page.wait_for_selector('button[data-testid="Component::Action-AssignmentPaymentsButton--0"]', timeout=30000)
            
            with page.expect_download() as download_info_pagos:
                page.get_by_test_id("Component::Action-AssignmentPaymentsButton--0").click()
            
            download_p = download_info_pagos.value
            download_p.save_as(file_pagos)
            print(f"✅ Pagos guardados.")

            # --- PARTE 3: LOGS (HEAD) ---
            print("\n--- 📊 DATA PREVIEW ---")
            for name, path in [("ASIGNACIONES", file_asignaciones), ("PAGOS", file_pagos)]:
                if os.path.exists(path):
                    df = pd.read_csv(path)
                    print(f"\n[{name}] - Filas: {len(df)}")
                    print(df.head(3))

            context.storage_state(path=state_path)
            print("\n🔄 Sesión renovada.")

        except Exception as e:
            print(f"❌ Error: {e}")
            raise e
        finally:
            browser.close()

if __name__ == "__main__":
    run_extraction()