from dataclasses import dataclass
import pandas as pd
import psycopg2

from sql_db import DATABASE_URL, get_num_rows
from utils.logging_utils import logger


def create_image_table():
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('images',))
    if cursor.fetchone()[0]:
        pass
    else:
        cursor.execute(
            '''
            CREATE TABLE images (image_id SERIAL PRIMARY KEY,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                image_uid TEXT UNIQUE,
                                user_id INTEGER,
                                prompt TEXT,
                                negative_prompt TEXT,
                                seed INTEGER,
                                gs REAL,
                                steps INTEGER,
                                idx INTEGER,
                                num_generated INTEGER,
                                scheduler_cls TEXT,
                                model_id TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(user_id))
            ''')
        conn.commit()
        logger.info("Created table images")
    cursor.close()
    conn.close()


@dataclass(frozen=True)
class ImageData:
    image_uid: str
    user_id: int
    prompt: str
    negative_prompt: str
    seed: int
    gs: float
    steps: int
    idx: int
    num_generated: int
    scheduler_cls: str
    model_id: str


def add_image(image_data: ImageData):
    prompt = image_data.prompt.replace("'", "[single_quote]")
    image_uid = image_data.image_uid
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM images WHERE image_uid=%s", (image_uid,))
    image = cursor.fetchone()
    if image is not None:
        pass
    else:
        cursor.execute(
            "INSERT INTO images (image_uid, user_id, prompt, negative_prompt, seed, gs, steps, idx, num_generated, scheduler_cls, model_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (image_uid, image_data.user_id, prompt, image_data.negative_prompt, image_data.seed, image_data.gs,
             image_data.steps, image_data.idx, image_data.num_generated, image_data.scheduler_cls, image_data.model_id))

        conn.commit()
        # logger.debug(f"Added image with uid {image_uid}")
    cursor.close()
    conn.close()
    return


def get_all_images(start_date=None) -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    if start_date is not None:
        cursor.execute("SELECT * FROM images WHERE created_at >= %s", (start_date,))
    else:
        cursor.execute("SELECT * FROM images")
    images = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(images, columns=['image_id',
                                       'created_at',
                                       'image_uid',
                                       'user_id',
                                       'prompt',
                                       'negative_prompt',
                                       'seed',
                                       'gs',
                                       'steps',
                                       'idx',
                                       'num_generated',
                                       'scheduler_cls',
                                       'model_id'])
    return df


def get_num_images() -> int:
    num_rows = get_num_rows("images")
    return num_rows
