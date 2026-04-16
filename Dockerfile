FROM apache/airflow:2.8.1

USER root

# Instalamos TODAS las dependencias de sistema requeridas
RUN apt-get update && apt-get install -y \
    libxfixes3 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgbm1 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxkbcommon0 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalamos Playwright en el entorno de Python
RUN pip install --no-cache-dir playwright playwright-stealth

# Instalamos los binarios de los navegadores
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/airflow/include/playwright_browsers
RUN playwright install chromium