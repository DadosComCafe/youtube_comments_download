import pyarrow.parquet as pq
from psycopg2 import connect
import json
import logging


def get_comments_from_parquet(youtube_credentials: dict) -> list:
    video_id = youtube_credentials["VIDEO_ID"]
    file_path = f"parquet_files/{video_id}.parquet"

    try:
        logging.info(f"Getting comments from Parquet file for video_id: {video_id}")
        table = pq.read_table(file_path)
        data = table.to_pandas()
        logging.info(f"Here: {data.columns}")
        logging.info(f"Successfully read comments from Parquet file.")
        return data
    except Exception as e:
        raise Exception(f"Error reading Parquet file: {e}")


def insert_comment_to_postgres(postgres_credentials: dict, youtube_credentials: dict):
    data = get_comments_from_parquet(youtube_credentials)
    conn = connect(
        database="youtube",
        user=postgres_credentials["POSTGRES_USER"],
        password=postgres_credentials["POSTGRES_PASSWORD"],
        host=postgres_credentials["POSTGRES_HOST"],
    )
    cur = conn.cursor()
    try:
        for index, row in data.iterrows():
            logging.info(f"Sample of item: {row}")
            sql = (
                "INSERT INTO comment (comment, username, replies) VALUES (%s, %s, %s);"
            )
            cur.execute(
                sql,
                (row["comment"], row["user_name"], json.dumps(row["replies"].tolist())),
            )
            conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise Exception(f"Error inserting in postgres: {e} -> {index}")


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
