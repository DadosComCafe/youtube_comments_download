from jinja2 import Environment, FileSystemLoader
from pymongo import MongoClient
import yaml
import os
import logging


logging.basicConfig(level=logging.INFO)


def get_inputs_from_mongodb(dict_credentials: dict, **kwargs) -> None:
    connection_config = f"""mongodb://{dict_credentials["user"]}:{dict_credentials["password"]}@{dict_credentials["host"]}:{dict_credentials["port"]}/"""
    client = MongoClient(connection_config)
    db = client["videos_dag"]
    collection = db["videos_to_analyse"]
    list_of_videos = []
    for video in collection.find({"is_dag": False}):
        logging.info(f"Video id: {video['video_id']}")
        list_of_videos.append(video["video_id"])
    kwargs["ti"].xcom_push(key="list_of_video_ids", value=list_of_videos)


def write_yaml_files(**kwargs):
    list_of_video_ids = kwargs["ti"].xcom_pull(
        task_ids="get_list_video_id", key="list_of_video_ids"
    )
    for id in list_of_video_ids:
        os.makedirs("dags/tasks/include/input/", exist_ok=True)

        yaml_path_file = f"dags/tasks/include/input/config_{id}.yaml"
        template_yamlfile = {
            "video_id": id,
            "schedule_interval": "@daily",
            "catchup": False,
        }
        logging.info(f"Generating file: {id}")
        try:
            with open(yaml_path_file, "w") as file:
                yaml.dump(template_yamlfile, file, default_flow_style=False)
            logging.info(f"File has been generated!")
        except Exception as e:
            logging.error(f"An exception: {e}")


def generate_dags():
    base_dir = "dags/tasks/include"
    env = Environment(loader=FileSystemLoader(base_dir))
    template = env.get_template("templates/dag_template.jinja2")

    for file in os.listdir(f"{base_dir}/input"):
        logging.info(file)
        if file.endswith(".yaml"):
            with open(f"{base_dir}/input/{file}", "r") as input:
                inputs = yaml.safe_load(input)
                with open(f"dags/youtube_{inputs['video_id']}.py", "w") as file2:
                    file2.write(template.render(inputs))
