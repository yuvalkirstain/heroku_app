import sqlite3
from dataclasses import dataclass
import pandas as pd
import psycopg2

from sql_db import DATABASE_URL
from utils import logger


def create_user_table():
    # Create table if it doesn't already exist
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('users',))
    if cursor.fetchone()[0]:
        logger.info("Table users already exists")
    else:
        cursor.execute(
            '''
            CREATE TABLE users (user_id SERIAL PRIMARY KEY,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                email TEXT UNIQUE,
                                name TEXT)
            ''')
        conn.commit()
        logger.info("Created table users")
    cursor.close()
    conn.close()


@dataclass
class UserSchema:
    user_id: str
    created_at: str
    email: str
    name: str


def add_user(email: str, name: str):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    user = get_user_by_email(email)
    if user is None:
        logger.info(f"Adding user {name} with email {email}")
        cursor.execute(f"INSERT INTO users (email, name) VALUES ('{email}', '{name}')")
        conn.commit()
    else:
        logger.info(f"User {name} with email {email} already exists")
    cursor.close()
    conn.close()


def get_user_by_email(email: str):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM users WHERE email=%s", (email,))
    user = cursor.fetchone()
    if user is None:
        return None
    cursor.close()
    conn.close()
    return UserSchema(*user) if user is not None else None


def get_users_by_name(name: str):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM users WHERE name=%s", (name,))
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users


def get_all_users() -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM users")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(users, columns=['user_id', 'created_at', 'email', 'name'])
    return df
