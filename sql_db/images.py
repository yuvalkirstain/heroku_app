import sqlite3
from dataclasses import dataclass
import pandas as pd
from sql_db.const import DB_FILE


def create_image_table():
    db = sqlite3.connect(DB_FILE)
    try:
        db.execute(f"SELECT * FROM images").fetchall()
    except sqlite3.OperationalError:
        db.execute(
            '''
            CREATE TABLE images (image_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                image_hash TEXT UNIQUE,
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
        db.commit()
    db.close()


@dataclass(frozen=True)
class ImageData:
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
    image_hash = hash(image_data)
    db = sqlite3.connect(DB_FILE)
    if db.execute(f"SELECT * FROM images WHERE image_hash = {image_hash}").fetchone():
        print(f"Image with hash {image_hash} exists")
    else:
        db.execute(f"INSERT INTO images (image_hash, user_id, prompt, negative_prompt, seed, gs, steps, idx, num_generated, scheduler_cls, model_id) VALUES ({image_hash}, {image_data.user_id}, '{image_data.prompt}', '{image_data.negative_prompt}', {image_data.seed}, {image_data.gs}, {image_data.steps}, {image_data.idx}, {image_data.num_generated}, '{image_data.scheduler_cls}', '{image_data.model_id}')")
        db.commit()
        print(f"Added image with hash {image_hash}")
    db.close()


def get_all_images() -> pd.DataFrame:
    db = sqlite3.connect(DB_FILE)
    images = db.execute(f"SELECT * FROM images").fetchall()
    db.close()
    df = pd.DataFrame(images, columns=['image_id',
                                       'created_at',
                                       'image_hash',
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
