FROM apache/airflow:3.1.8

# pytest pour lancer la suite de tests dans le conteneur worker ;
# apache-airflow est ré-épinglé pour ne jamais être mis à jour par pip.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /tmp/requirements-airflow.txt
