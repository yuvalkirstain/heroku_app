import psycopg2
from dataclasses import dataclass
import pandas as pd

from sql_db import DATABASE_URL, get_num_rows
from utils.logging_utils import logger


def create_rankings_table():
    # Create table if it doesn't already exist
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('rankings',))
    if cursor.fetchone()[0]:
        pass
    else:
        cursor.execute(
            '''
            CREATE TABLE rankings (ranking_id SERIAL PRIMARY KEY,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                    user_id INTEGER,
                                    image_0_uid TEXT,
                                    image_1_uid TEXT,
                                    image_2_uid TEXT,
                                    image_3_uid TEXT,
                                    best_image_uid TEXT,
                                    prompt TEXT,
                                    FOREIGN KEY(user_id) REFERENCES users(user_id))
            ''')
        conn.commit()
        logger.info("Created table rankings")
    cursor.close()
    conn.close()


@dataclass
class RankingSchema:
    ranking_id: str
    created_at: str
    user_id: int
    image_0_uid: str
    image_1_uid: str
    image_2_uid: str
    image_3_uid: str
    best_image_uid: str
    prompt: str


@dataclass
class RankingData:
    user_id: int
    image_0_uid: str
    image_1_uid: str
    image_2_uid: str
    image_3_uid: str
    best_image_uid: str
    prompt: str


def add_ranking(ranking: RankingData):
    prompt = ranking.prompt.replace("'", "[single_quote]")
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO rankings (user_id, image_0_uid, image_1_uid, image_2_uid, image_3_uid, best_image_uid, prompt) VALUES ({ranking.user_id}, '{ranking.image_0_uid}', '{ranking.image_1_uid}', '{ranking.image_2_uid}', '{ranking.image_3_uid}', '{ranking.best_image_uid}', '{prompt}')")
    conn.commit()
    cursor.close()
    conn.close()


def get_all_rankings() -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM rankings")
    rankings = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(rankings,
                      columns=['ranking_id', 'created_at', 'user_id', 'image_1_uid', 'image_2_uid', 'image_3_uid',
                               'image_4_uid', 'best_image_uid', 'prompt'])
    return df


def get_num_rankings() -> int:
    num_rows = get_num_rows("rankings")
    return num_rows
