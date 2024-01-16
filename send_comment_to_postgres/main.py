from psycopg2 import connect
import logging
import json


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
