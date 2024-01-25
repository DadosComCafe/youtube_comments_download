import json
from psycopg2 import connect
from tasks.custom_data_structure import CommentThread
import logging


def get_comments_from_json(youtube_credentials: dict) -> list:
    video_id = youtube_credentials["VIDEO_ID"]
    file_path = f"json_files/{video_id}.json"
    logging.info(f"Getting comments from json file for video_id: {video_id}")
    with open(file_path, "r") as file:
        data = file.read()
    data = json.loads(data)
    return data


def create_comment_thread_objects(youtube_credentials: dict) -> list:
    json_data = get_comments_from_json(youtube_credentials)
    comment_thread_objects = [CommentThread(**item) for item in json_data]
    logging.info(f"A sample of comment_thread: {comment_thread_objects[0]}")
    return comment_thread_objects


def insert_comment_snippet(cursor, comment_snippet_data):
    insert_query = """
        INSERT INTO CommentSnippet (
            id, channelId, videoId, textDisplay, textOriginal,
            authorDisplayName, authorProfileImageUrl, authorChannelUrl,
            authorChannelId, canRate, viewerRating, likeCount,
            publishedAt, updatedAt
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(
        insert_query,
        (
            comment_snippet_data.id,
            comment_snippet_data.snippet.channelId,
            comment_snippet_data.snippet.videoId,
            comment_snippet_data.snippet.topLevelComment.snippet.textDisplay,
            comment_snippet_data.snippet.topLevelComment.snippet.textOriginal,
            comment_snippet_data.snippet.topLevelComment.snippet.authorDisplayName,
            comment_snippet_data.snippet.topLevelComment.snippet.authorProfileImageUrl,
            comment_snippet_data.snippet.topLevelComment.snippet.authorChannelUrl,
            comment_snippet_data.snippet.topLevelComment.snippet.authorChannelId[
                "value"
            ],
            comment_snippet_data.snippet.topLevelComment.snippet.canRate,
            comment_snippet_data.snippet.topLevelComment.snippet.viewerRating,
            comment_snippet_data.snippet.topLevelComment.snippet.likeCount,
            comment_snippet_data.snippet.topLevelComment.snippet.publishedAt,
            comment_snippet_data.snippet.topLevelComment.snippet.updatedAt,
        ),
    )


def insert_comment_snippet_to_postgres(
    postgres_credentials: dict, youtube_credentials: dict
):
    items_content = create_comment_thread_objects(youtube_credentials)
    logging.info(f"Exemplo de items_content: {items_content[0]}")
    conn = connect(
        database="youtube",
        user=postgres_credentials["POSTGRES_USER"],
        password=postgres_credentials["POSTGRES_PASSWORD"],
        host=postgres_credentials["POSTGRES_HOST"],
    )
    cur = conn.cursor()
    try:
        for item in items_content:
            logging.info(f"Exemplo de item: {item.snippet}")
            insert_comment_snippet(cursor=cur, comment_snippet_data=item)
            conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Deu ruim {e}")


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
