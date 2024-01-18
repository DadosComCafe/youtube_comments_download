FROM apache/airflow:latest-python3.11
USER airflow
RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install -r requirements.txt
