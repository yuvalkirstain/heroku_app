import psycopg2
from dataclasses import dataclass
import pandas as pd

from sql_db import DATABASE_URL
from utils.logging_utils import logger


def create_downloads_table():
    # Create table if it doesn't already exist
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('downloads',))
    if cursor.fetchone()[0]:
        logger.info("Table downloads already exists")
    else:
        cursor.execute(
            '''
            CREATE TABLE downloads (download_id SERIAL PRIMARY KEY,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                    user_id INTEGER,
                                    image_uid TEXT,
                                    prompt TEXT,
                                    FOREIGN KEY(user_id) REFERENCES users(user_id))
            ''')
        conn.commit()
        logger.info("Created table downloads")
    cursor.close()
    conn.close()


@dataclass
class DownloadSchema:
    download_id: str
    created_at: str
    user_id: int
    image_uid: str
    prompt: str


@dataclass
class DownloadData:
    user_id: int
    image_uid: str
    prompt: str


def add_download(download: DownloadData):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO downloads (user_id, image_uid, prompt) VALUES ({download.user_id}, '{download.image_uid}', '{download.prompt}')")
    conn.commit()
    cursor.close()
    conn.close()


def get_all_downloads() -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM downloads")
    rankings = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(rankings,
                      columns=['download_id', 'created_at', 'user_id', 'image_uid', 'prompt'])
    return df
