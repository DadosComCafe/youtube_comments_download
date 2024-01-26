import os
import json
import logging
import pandas as pd
from googleapiclient.discovery import build
from google.cloud.storage import Client


logging.basicConfig(level=logging.INFO)


def get_video_content(youtube_credentials: dict) -> list | str:
    video_id = youtube_credentials.get("VIDEO_ID")
    api_key = youtube_credentials.get("API_KEY")
    logging.info(f"Requesting from {video_id} using the key {api_key}")
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.commentThreads().list(
        part="snippet, replies", videoId=video_id, textFormat="plainText"
    )
    df1 = pd.DataFrame(columns=["comment", "replies", "date", "username"])
    if not os.path.exists("json_files"):
        os.makedirs("json_files")
    n = 0
    while request:
        replies = []
        comments = []
        dates = []
        usernames = []

        try:
            response = request.execute()
            for item in response.get("items"):

                comment = (
                    item.get("snippet", {})
                    .get("topLevelComment", {})
                    .get("snippet", {})
                    .get("textDisplay")
                )
                comments.append(comment)

                user_name = (
                    item.get("snippet", {})
                    .get("topLevelComment", {})
                    .get("snippet", {})
                    .get("authorDisplayName")
                )
                usernames.append(user_name)

                date = (
                    item.get("snippet")
                    .get("topLevelComment", {})
                    .get("snippet", {})
                    .get("publishedAt")
                )
                dates.append(date)

                replycount = item.get("snippet", {}).get("totalReplyCount")

                if replycount > 0:
                    replies.append([])
                    for reply in item.get("replies", {}).get("comments", []):
                        reply = reply.get("snippet", {}).get("textDisplay")
                        replies[-1].append(reply)
                else:
                    replies.append([])

            df2 = pd.DataFrame(
                {
                    "comment": comments,
                    "replies": replies,
                    "user_name": usernames,
                    "date": dates,
                }
            )
            df3 = pd.concat([df1, df2], ignore_index=False)
            df3.to_json(f"json_files/{video_id}.json", index=True)
            request = youtube.commentThreads().list_next(request, response)
            n += 1
            logging.info(f"Iterating {n}")
        except Exception as e:
            logging.info(f"An error: {e}")
            break


def export_to_json(items_content: json, video_id: str) -> None:
    file_path = f"json_files/{video_id}.json"
    if not items_content:
        return
    if not os.path.exists(file_path.split("/")[0]):
        os.makedirs(file_path.split("/")[0])
    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(items_content, file, indent=2)


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
