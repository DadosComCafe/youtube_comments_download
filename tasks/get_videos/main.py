import requests
import os
import json
import logging
from pymongo import MongoClient, UpdateOne
from math import ceil
from google.cloud.storage import Client
from tasks.get_videos.utils import chunkify

logging.basicConfig(level=logging.INFO)


def export_to_json(items_content: json, video_id: str) -> None:
    file_path = f"json_files/{video_id}.json"
    if not items_content:
        return
    if not os.path.exists(file_path.split("/")[0]):
        os.makedirs(file_path.split("/")[0])
    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(items_content, file, indent=2)


def get_video_content(youtube_credentials: dict) -> list | str:
    video_id = youtube_credentials["VIDEO_ID"]
    api_key = youtube_credentials["API_KEY"]
    logging.info(f"Requesting from {video_id} using the key {api_key}")
    page_token = ""
    target_url = f"https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&part=replies&pageToken={page_token}&videoId={video_id}&key={api_key}&alt=json"
    video_request = requests.get(target_url)
    json_content = video_request.json()

    if video_request.status_code == 200:
        if "nextPageToken" not in json_content:
            items_content = json_content["items"]
            return items_content, items_content[0]["snippet"]["videoId"]

        next_page_token = json_content.get("nextPageToken")
        items_content = []

        while next_page_token != "":
            page_token = next_page_token
            target_url = f"https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&part=replies&pageToken={page_token}&videoId={video_id}&key={api_key}&alt=json"
            json_content = requests.get(target_url).json()
            items_content.extend(json_content["items"])
            next_page_token = json_content.get("nextPageToken", "")
            logging.info(f"Using the token to get next page: {next_page_token}")
        logging.info(f"Total number of records: {len(items_content)}")
        export_to_json(items_content=items_content, video_id=video_id)
        return items_content

    return "Video fora do ar!"


# def send_json_to_mongo(dict_credentials: dict, items_content: json, video_id: str):
#     connection_config = f"""mongodb://{dict_credentials["mongo_user"]}:{dict_credentials["mongo_password"]}@{dict_credentials["mongo_host"]}:{dict_credentials["mongo_port"]}/"""
#     client = MongoClient(connection_config)
#     collection = client["youtube_comments"][video_id]
#     try:
#         operations = []
#         for item in items_content:
#             id_comment = item["snippet"]["topLevelComment"]["id"]
#             filter_comment_id = {"commentId": id_comment}

#             operations.append(UpdateOne(filter_comment_id, {"$set": item}, upsert=True))
#         for index, chunk in enumerate(chunkify(operations, 100)):
#             n_ops = ceil(len(operations) / 100)
#             logging.info(
#                 f"Executing inserting in collection, chunk {index + 1}/{n_ops}"
#             )
#             result = collection.bulk_write(chunk, ordered=False)
#             logging.info(f"{result.bulk_api_result}\n")
#         logging.info(
#             f"The json content has been inserted successfully for the video_id: {video_id}"
#         )
#     except Exception as e:
#         raise Exception(f"An error: {e}")


def export_json_parquet(items_content: json, video_id: str):
    from pandas import DataFrame
    from pyarrow import parquet, Table

    temp_df = DataFrame(items_content)
    table = Table.from_pandas(temp_df)
    file_path = f"videos_parquet/{video_id}.parquet"
    try:
        parquet.write_table(table, file_path)
        logging.info(f"The parquet file to {video_id} has been exported successfully!")
    except Exception as e:
        logging.error(f"An error {e}")


def upload_json_to_storage(cloud_storage_credentials: dict, youtube_credentials: dict):
    try:
        client_storage = Client.from_service_account_info(cloud_storage_credentials)
        logging.info("The client storage has been created successfully!")
    except Exception as e:
        raise Exception(f"An error {e}")
    video_id = youtube_credentials["VIDEO_ID"]
    file_path = f"json_files/{video_id}.json"
    try:
        bucket = client_storage.bucket(
            "estudo-63ee3.appspot.com"
        )  # TODO passar o bucket como variável de ambiente
        logging.info("Using the bucket!")
        blob = bucket.blob(f"json_files/{video_id}.json")
        blob.upload_from_filename(file_path)
        logging.info(f"The file has been uploaded successfully!")
    except Exception as e:
        raise Exception(f"An error {e}")


if __name__ == "__main__":
    from decouple import config

    my_api_key = config("API_KEY")
    dict_credentials = {
        "mongo_user": config("MONGO_USERNAME"),
        "mongo_password": config("MONGO_PASSWORD"),
        "mongo_host": config("MONGO_HOST"),
        "mongo_port": config("MONGO_PORT"),
    }
    video_id = "PzUmRTcozms"
    video_content = get_video_content(video_id=video_id, api_key=my_api_key)
    send_json_to_mongo(
        dict_credentials=dict_credentials,
        items_content=video_content,
        video_id=video_id,
    )
