import os

import psycopg2
from dataclasses import dataclass
import pandas as pd

from sql_db import DATABASE_URL
from utils import logger


def create_rankings_table():
    # Create table if it doesn't already exist
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('rankings',))
    if cursor.fetchone()[0]:
        logger.info("Table rankings already exists")
    else:
        cursor.execute(
            '''
            CREATE TABLE rankings (ranking_id SERIAL PRIMARY KEY,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                    user_id INTEGER,
                                    image_0_hash TEXT,
                                    image_1_hash TEXT,
                                    image_2_hash TEXT,
                                    image_3_hash TEXT,
                                    best_image_hash TEXT,
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
    image_0_hash: str
    image_1_hash: str
    image_2_hash: str
    image_3_hash: str
    best_image_hash: str


@dataclass
class RankingData:
    user_id: int
    image_0_hash: str
    image_1_hash: str
    image_2_hash: str
    image_3_hash: str
    best_image_hash: str


def add_ranking(ranking: RankingData):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(
        f"INSERT INTO rankings (user_id, image_0_hash, image_1_hash, image_2_hash, image_3_hash, best_image_hash) VALUES ({ranking.user_id}, '{ranking.image_0_hash}', '{ranking.image_1_hash}', '{ranking.image_2_hash}', '{ranking.image_3_hash}', '{ranking.best_image_hash}')")
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
                      columns=['ranking_id', 'created_at', 'user_id', 'image_1_hash', 'image_2_hash', 'image_3_hash',
                               'image_4_hash', 'best_image_hash'])
    return df
