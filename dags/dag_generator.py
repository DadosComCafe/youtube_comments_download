from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.include.script.dag_generator import generate_dags

with DAG(
    dag_id="dag_generator",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_generate_dags = PythonOperator(
        task_id="generate_dags",
        python_callable=generate_dags,
        dag=dag,
    )

    task_generate_dags
