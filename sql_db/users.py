import hashlib
from dataclasses import dataclass
import pandas as pd
import psycopg2

from sql_db import DATABASE_URL, get_num_rows
from utils.logging_utils import logger


def create_user_table():
    # Create table if it doesn't already exist
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('users_anon',))
    if cursor.fetchone()[0]:
        pass
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
    name = hashlib.md5(name.encode('utf-8')).hexdigest()
    if user is None:
        logger.info(f"Adding user {name} with email {email}")
        mail_hash = hashlib.md5(email.encode('utf-8')).hexdigest()
        cursor.execute("INSERT INTO users_anon (email, name) VALUES (%s, %s)", (mail_hash, name))
        conn.commit()
        user = get_user_by_email(mail_hash)
    else:
        logger.info(f"User {name} with email {email} already exists")
    cursor.close()
    conn.close()
    return user.user_id


def get_user_by_email(email: str):
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    mail_hash = hashlib.md5(email.encode('utf-8')).hexdigest()
    cursor.execute(f"SELECT * FROM users_anon WHERE email=%s", (mail_hash,))
    user = cursor.fetchone()
    if user is None:
        return None
    cursor.close()
    conn.close()
    return UserSchema(*user) if user is not None else None


def get_users_by_name(name: str):
    name = hashlib.md5(name.encode('utf-8')).hexdigest()
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM users_anon WHERE name=%s", (name,))
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users


def get_all_users() -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM users_anon")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(users, columns=['user_id', 'created_at', 'email', 'name'])
    return df


def get_num_users() -> int:
    num_rows = get_num_rows("users")
    return num_rows
