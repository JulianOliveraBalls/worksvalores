import os
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright

def run_extraction():
    base_path = "/opt/airflow/include"
    state_path = f"{base_path}/state.json"
    
    # Rutas finales (usamos /tmp para que el DAG las borre)
    file_asignaciones = "/opt/airflow/shared/reporte_asignaciones.csv"
    file_pagos = "/opt/airflow/shared/reporte_pagos.csv"

    with sync_playwright() as p:
        print("🚀 Iniciando navegador...")
        browser = p.chromium.launch(headless=True)
        
        # Agregamos viewports y camuflaje
        context = browser.new_context(
            storage_state=state_path,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        
        page = context.new_page()
        # Hack para ocultar que es un bot
        page.add_init_script("delete navigator.webdriver")

        try:
            # --- PARTE 1: ASIGNACIONES ---
            print("📥 Navegando a Asignaciones...")
            url_asig = "https://support.cashea.retool.com/apps/dc2f2576-0cea-11f1-bd08-17e6dccfb9af/Recuperaciones/Despachos%20-%20Asignaciones%20y%20Pagos/Asignaciones"
            
            # Vamos a la URL
            response = page.goto(url_asig, wait_until="load", timeout=60000)            
            # Si nos mandó al login, la URL habrá cambiado
            if "login" in page.url:
                page.screenshot(path=f"{base_path}/debug_AL_LOGIN.png")
                print("❌ ERROR: La sesión expiró y estamos en el LOGIN. Revisar debug_AL_LOGIN.png")
                return # Salimos para no romper nada

            print("Esperando botón de Asignaciones...")
            selector_asig = 'button[data-testid="Component::Action-AssignmentInstallmentsButton--0"]'
            page.wait_for_selector(selector_asig, timeout=60000)
            
            with page.expect_download(timeout=60000) as download_info:
                page.get_by_test_id("Component::Action-AssignmentInstallmentsButton--0").click()
            
            download_asig = download_info.value
            download_asig.save_as(file_asignaciones)
            print(f"✅ Asignaciones guardadas.")

            # --- PARTE 2: PAGOS ---
            print("📥 Navegando a pestaña de Pagos...")
            url_pagos = "https://support.cashea.retool.com/apps/dc2f2576-0cea-11f1-bd08-17e6dccfb9af/Recuperaciones/Despachos%20-%20Asignaciones%20y%20Pagos/Pagos"
            page.goto(url_pagos, wait_until="load", timeout=60000)
            
            print("Esperando botón de Pagos...")
            selector_pagos = 'button[data-testid="Component::Action-AssignmentPaymentsButton--0"]'
            page.wait_for_selector(selector_pagos, timeout=60000)
            
            with page.expect_download(timeout=60000) as download_info_pagos:
                page.get_by_test_id("Component::Action-AssignmentPaymentsButton--0").click()
            
            download_p = download_info_pagos.value
            download_p.save_as(file_pagos)
            print(f"✅ Pagos guardados.")

            # --- SOLO SI LLEGÓ ACÁ ACTUALIZAMOS LA SESIÓN ---
            context.storage_state(path=state_path)
            print("\n🔄 Sesión renovada con éxito.")

        except Exception as e:
            page.screenshot(path=f"{base_path}/error_ultimo_intento.png")
            print(f"❌ Error crítico: {e}. Foto guardada en error_ultimo_intento.png")
            raise e
        finally:
            browser.close()

if __name__ == "__main__":
    run_extraction()