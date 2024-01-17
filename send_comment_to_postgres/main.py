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


def insert_comments_to_postgres(postgres_credentials: dict, items_content: list[dict]):
    conn = connect(
        database="youtube",
        user=postgres_credentials["user"],
        password=postgres_credentials["password"],
        host=postgres_credentials["host"],
    )
    cur = conn.cursor()
    for item in items_content:
        keys = ", ".join(item.keys())
        values = ", ".join(["%s"] * len(item))
        query = f"INSERT INTO toplevelcomment ({keys}) VALUES ({values})"
        cur.execute(query, list(item.values()))
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

    collection_find_toplevelcomments = get_comments_from_mongo(
        mongo_credentials=mongo_credentials, video_id="PzUmRTcozms"
    )

    list_records = []
    for i in collection_find_toplevelcomments:
        list_records.append(
            {
                "id": i["snippet"]["topLevelComment"]["id"],
                "author_name": i["snippet"]["topLevelComment"]["snippet"][
                    "authorDisplayName"
                ],
                "text_original": i["snippet"]["topLevelComment"]["snippet"][
                    "textOriginal"
                ],
                "published_date": i["snippet"]["topLevelComment"]["snippet"][
                    "publishedAt"
                ],
            }
        )
    insert_comments_to_postgres(
        postgres_credentials=postgres_credentials, items_content=list_records
    )

    logging.info(f"Comentários total: {c}")
    logging.info(f"Comentários com réplicas: {d}")
    logging.info(f"Número de Réplicas: {e}")
    # logging.info(f"Comentários sem Réplicas: {c - d}")
