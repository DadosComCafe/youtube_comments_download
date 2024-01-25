from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.models import Variable
from tasks.get_videos import get_video_content, upload_json_to_storage
from tasks.send_comment_to_postgres import insert_comment_snippet_to_postgres

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

    postgres_credentials = {
        "POSTGRES_USER": Variable.get("POSTGRES_USER"),
        "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD"),
        "POSTGRES_HOST": Variable.get("POSTGRES_HOST"),
        "POSTGRES_PORT": Variable.get("POSTGRES_PORT"),
    }

    gcloud_credentials = {
        "type": Variable.get("type"),
        "project_id": Variable.get("project_id"),
        "private_key_id": Variable.get("private_key_id"),
        "private_key": Variable.get("private_key"),
        "client_email": Variable.get("client_email"),
        "client_id": Variable.get("client_id"),
        "auth_uri": Variable.get("auth_uri"),
        "token_uri": Variable.get("token_uri"),
        "auth_provider_x509_cert_url": Variable.get("auth_provider_x509_cert_url"),
        "client_x509_cert_url": Variable.get("client_x509_cert_url"),
        "universe_domain": Variable.get("universe_domain"),
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

    task_upload_json_to_storage = PythonOperator(
        task_id="upload_json",
        python_callable=upload_json_to_storage,
        op_args=[gcloud_credentials, youtube_credentials],
        dag=dag,
    )

    task_get_comments_from_json = PythonOperator(
        task_id="get_comments",
        python_callable=insert_comment_snippet_to_postgres,
        op_args=[postgres_credentials, youtube_credentials],
        dag=dag,
    )

    (
        task_initialize_dag
        >> Label("Initializing the dag")
        >> task_get_threadcomments
        >> Label("Making request to get threadcomments")
        >> task_upload_json_to_storage
        >> Label("Upload the generate files to cloud storage")
        >> task_get_comments_from_json
    )
