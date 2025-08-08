#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then #jika ada maka install, jika tidak ada maka skip
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

# # Create logs directory with proper permissions
# mkdir -p /opt/airflow/logs
# chmod -R 755 /opt/airflow/logs

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver

# Menjalankan perintah yang diberikan (bisa scheduler atau webserver)
# exec airflow "$@"
