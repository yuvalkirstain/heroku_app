from dataclasses import dataclass
import pandas as pd
import psycopg2

from sql_db import DATABASE_URL, get_num_rows
from utils.logging_utils import logger


def create_user_score_table():
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('user_score',))
    if cursor.fetchone()[0]:
        pass
    else:
        cursor.execute(
            '''
            CREATE TABLE user_score (user_id SERIAL PRIMARY KEY,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                score INTEGER)
            ''')
        conn.commit()
        logger.info("Created table user score")
    cursor.close()
    conn.close()


@dataclass
class UserScoreSchema:
    user_id: str
    created_at: str
    score: int


def increment_user_score(user_id: int):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM user_score WHERE user_id = %s", (user_id,))
    if cursor.fetchone() is None:
        cursor.execute("INSERT INTO user_score (user_id, score) VALUES (%s, %s)", (user_id, 1))
        conn.commit()
        logger.info(f"User {user_id} created and score incremented to 1.")
    else:
        cursor.execute("UPDATE user_score SET score = score + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
        # logger.info(f"User {user_id} score incremented.")
    cursor.close()
    conn.close()


def get_user_score(user_id: int) -> int:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, created_at, score FROM user_score WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result is None:
        return 0
    user_score = UserScoreSchema(*result)
    return user_score.score

