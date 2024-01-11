from get_videos import get_video_content, send_json_to_mongo
from decouple import config

dict_credentials = {
    "api_key": config("API_KEY"),
    "mongo_user": config("MONGO_USERNAME"),
    "mongo_password": config("MONGO_PASSWORD"),
    "mongo_port": config("MONGO_PORT"),
    "mongo_host": config("MONGO_HOST")
}

video_id = "PzUmRTcozms"
video_content = get_video_content(video_id=video_id, api_key=dict_credentials["api_key"])
send_json_to_mongo(dict_credentials=dict_credentials, items_content=video_content, video_id=video_id)