import sqlite3
from dataclasses import dataclass
import pandas as pd
from sql_db.const import DB_FILE


def create_rankings_table():
    # Create table if it doesn't already exist
    db = sqlite3.connect(DB_FILE)
    try:
        db.execute(f"SELECT * FROM rankings").fetchone()
    except sqlite3.OperationalError:
        db.execute(
            '''
            CREATE TABLE rankings (ranking_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                    user_id INTEGER,
                                    image_0_hash TEXT,
                                    image_1_hash TEXT,
                                    image_2_hash TEXT,
                                    image_3_hash TEXT,
                                    best_image_hash TEXT,
                                    FOREIGN KEY(user_id) REFERENCES users(user_id))
            ''')
        db.commit()
    db.close()


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
    db = sqlite3.connect(DB_FILE)
    db.execute(f"INSERT INTO rankings (user_id, image_0_hash, image_1_hash, image_2_hash, image_3_hash, best_image_hash) VALUES ({ranking.user_id}, '{ranking.image_0_hash}', '{ranking.image_1_hash}', '{ranking.image_2_hash}', '{ranking.image_3_hash}', '{ranking.best_image_hash}')")
    db.commit()
    db.close()


def get_all_rankings() -> pd.DataFrame:
    db = sqlite3.connect(DB_FILE)
    rankings = db.execute(f"SELECT * FROM rankings").fetchall()
    db.close()
    df = pd.DataFrame(rankings, columns=['ranking_id', 'created_at', 'user_id', 'image_1_hash', 'image_2_hash', 'image_3_hash', 'image_4_hash', 'best_image_hash'])
    return df
