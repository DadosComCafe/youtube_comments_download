from pymongo import MongoClient
from psycopg2 import connect
import logging
import json


def get_comments_from_mongo(
    mongo_credentials: dict, video_id: str, dict_query: dict = {}
) -> MongoClient:
    connection_config = f"""mongodb://{mongo_credentials["mongo_user"]}:{mongo_credentials["mongo_password"]}@{mongo_credentials["mongo_host"]}:{mongo_credentials["mongo_port"]}/"""
    client = MongoClient(connection_config)
    collection = client["youtube_comments"][video_id]
    return collection.find(dict_query)


def insert_comments_to_postgres(
    postgres_credentials: dict, items_content: json, video_id: str
):
    conn = connect(
        user=postgres_credentials["user"],
        password=postgres_credentials["password"],
        host=postgres_credentials["host"],
    )
    cur = conn.cursor()
    # for item in items_content:
    # TODO usar os modelos do pydantic
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from decouple import config

    postgres_credentials = {
        "user": config("POSTGRES_USER"),
        "password": config("POSTGRES_PASSWORD"),
        "host": config("POSTGRES_HOST"),
        "port": config("POSTGRES_PORT"),
        "database": "youtube_comments",
    }

    mongo_credentials = {
        "api_key": config("API_KEY"),
        "mongo_user": config("MONGO_USERNAME"),
        "mongo_password": config("MONGO_PASSWORD"),
        "mongo_port": config("MONGO_PORT"),
        "mongo_host": config("MONGO_HOST"),
        "debug": config("DEBUG"),
    }

    collection_find_total = get_comments_from_mongo(
        mongo_credentials=mongo_credentials, video_id="PzUmRTcozms"
    )
    collection_find_childreen = get_comments_from_mongo(
        mongo_credentials=mongo_credentials,
        video_id="PzUmRTcozms",
        dict_query={"replies.comments": {"$exists": True}},
    )

    c, d, e = 0, 0, 0
    for i in collection_find_total:
        logging.info(i["snippet"]["topLevelComment"]["snippet"]["textOriginal"])
        c += 1
    for i in collection_find_childreen:
        d += 1
        for comment in i["replies"]["comments"]:
            e += 1
            logging.info(len(i["replies"]["comments"]))
            logging.info({comment["id"]: comment["snippet"]["textOriginal"]})

    logging.info(f"Comentários total: {c}")
    logging.info(f"Comentários com réplicas: {d}")
    logging.info(f"Número de Réplicas: {e}")
    logging.info(f"Comentários sem Réplicas: {c - d}")
