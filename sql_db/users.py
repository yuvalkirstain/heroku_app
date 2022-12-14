import sqlite3
from dataclasses import dataclass
import pandas as pd
from sql_db.const import DB_FILE


def create_user_table():
    # Create table if it doesn't already exist
    db = sqlite3.connect(DB_FILE)
    try:
        db.execute(f"SELECT * FROM users").fetchone()
    except sqlite3.OperationalError:
        db.execute(
            '''
            CREATE TABLE users (user_id INTEGER PRIMARY KEY,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                email TEXT UNIQUE,
                                name TEXT)
            ''')
        db.commit()


@dataclass
class UserSchema:
    user_id: str
    created_at: str
    email: str
    name: str


def add_user(email: str, name: str):
    db = sqlite3.connect(DB_FILE)
    user = get_user_by_email(email)
    if user is None:
        print(f"Adding user {name} with email {email}")
        db.execute(f"INSERT INTO users (email, name) VALUES ('{email}', '{name}')")
        db.commit()
    else:
        print(f"User {name} with email {email} already exists")
    db.close()


def get_user_by_email(email: str):
    db = sqlite3.connect(DB_FILE)
    user = db.execute(f"SELECT * FROM users WHERE email='{email}'").fetchone()
    db.close()
    return UserSchema(*user) if user is not None else None


def get_users_by_name(name: str):
    db = sqlite3.connect(DB_FILE)
    users = db.execute(f"SELECT * FROM users WHERE name='{name}'").fetchall()
    db.close()
    return users


def get_all_users() -> pd.DataFrame:
    db = sqlite3.connect(DB_FILE)
    users = db.execute(f"SELECT * FROM users").fetchall()
    db.close()
    df = pd.DataFrame(users, columns=['user_id', 'created_at', 'email', 'name'])
    return df
