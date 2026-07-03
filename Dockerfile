FROM apache/airflow:3.2.2-python3.11

USER root

# Устанавливаем системные зависимости (если нужны)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Копируем requirements и устанавливаем
COPY requirements.txt /home/airflow/requirements.txt
RUN pip install --no-cache-dir -r /home/airflow/requirements.txt && \
    rm /home/airflow/requirements.txt

