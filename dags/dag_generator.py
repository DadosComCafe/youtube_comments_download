from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.models import Variable
from tasks.include.script.dag_generator import (
    get_inputs_from_mongodb,
    write_yaml_files,
    generate_dags,
)

with DAG(
    dag_id="dag_generator",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    mongodb_credentials = {
        "host": Variable.get("mongo_host"),
        "user": Variable.get("mongo_user"),
        "password": Variable.get("mongo_password"),
        "port": Variable.get("mongo_port"),
        "schema": "videos_dag",
    }

    task_initialize_dag = BashOperator(
        task_id="initializing_dag", bash_command="echo Initializing Dag!"
    )

    task_get_list_video_id = PythonOperator(
        task_id="get_list_video_id",
        python_callable=get_inputs_from_mongodb,
        op_args=[mongodb_credentials],
        dag=dag,
    )

    task_write_yaml_files = PythonOperator(
        task_id="write_yaml_files", python_callable=write_yaml_files, dag=dag
    )

    task_generate_dags = PythonOperator(
        task_id="generate_dags",
        python_callable=generate_dags,
        dag=dag,
    )

    (
        task_initialize_dag
        >> Label("Initializing the dag")
        >> task_get_list_video_id
        >> Label("Getting the video id to create new dags")
        >> task_write_yaml_files
        >> Label("Creating the yaml files to use in the dag generation")
        >> task_generate_dags
    )
