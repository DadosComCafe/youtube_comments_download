from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.models import Variable
from tasks.get_videos import get_video_content

with DAG(
    dag_id="youtube_video_analysis",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:

    youtube_credentials = {
        "API_KEY": Variable.get("API_KEY"),
        "VIDEO_ID": Variable.get("VIDEO_ID"),
    }

    task_initialize_dag = BashOperator(
        task_id="initializing_dag", bash_command="echo Initializing Dag!"
    )

    task_get_threadcomments = PythonOperator(
        task_id="task_get_threadcomments",
        python_callable=get_video_content,
        op_args=[youtube_credentials],
        dag=dag,
    )

    task_send_content_to_mongo = PythonOperator

    (
        task_initialize_dag
        >> Label("Making request to get threadcomments")
        >> task_get_threadcomments
    )
