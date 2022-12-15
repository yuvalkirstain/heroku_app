import os

DB_NAME = "natali"
DB_FILE = f"./{DB_NAME}.db"
HF_DB_DIR = "./data"
HF_DB_PATH = f"{HF_DB_DIR}/{DB_NAME}.db"
TOKEN = os.environ.get('HUB_TOKEN')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
REPO_NAME = f"https://yuvalkirstain:{GITHUB_TOKEN}@github.com/yuvalkirstain/natali_data.git"
DATABASE_URL = os.environ['DATABASE_URL']
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
BUCKET_NAME = "text-to-image-human-preferences"
S3_EXTRA_ARGS = {'ACL': 'public-read'}
IMAGE_DIR = "images"
os.makedirs(IMAGE_DIR, exist_ok=True)
