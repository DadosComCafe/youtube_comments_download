import requests
import json
import logging
from pymongo import MongoClient

def get_video_content(video_id: str, api_key: str) -> tuple:
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
        return items_content, json_content["items"][0]["snippet"]["videoId"]

    return "Video fora do ar!"

def send_json_to_mongo(dict_credentials: dict, items_content: json, video_id: str):
    connection_config = f"mongodb://{dict_credentials["mongo_user"]}:{dict_credentials["mongo_password"]}@{dict_credentials["mongo_host"]}:{dict_credentials["mongo_port"]}/"
    client = MongoClient(connection_config)
    collection = client["youtube_comments"][video_id]
    try:
        for item in items_content:
            collection.insert_one(item)
        logging.info(f"The json content has been inserted successfully for the video_id: {video_id}")
    except Exception as e:
        raise Exception(f"An error: {e}")




if __name__ == "__main__":
    from decouple import config
    import ipdb
    my_api_key = config("API_KEY")
    dict_credentials = {
        "mongo_user": config("MONGO_USERNAME"),
        "mongo_password": config("MONGO_PASSWORD"),
        "mongo_host": config("MONGO_HOST"),
        "mongo_port": config("MONGO_PORT")
    }
    video_content, video_id = get_video_content(video_id="PzUmRTcozms", api_key=my_api_key)
    #ipdb.set_trace()

    send_json_to_mongo(dict_credentials=dict_credentials, items_content=video_content, video_id=video_id)