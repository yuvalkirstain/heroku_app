import asyncio
import base64
import collections
import json
import os
import re
import time
import uuid
from io import BytesIO
from typing import List, Union, Tuple, Optional
from urllib.parse import urlparse

import aiohttp
import boto3
import requests
from PIL import Image
from pydantic import BaseModel, Field
from apscheduler.schedulers.background import BackgroundScheduler

from sql_db.user_score import get_user_score, increment_user_score, create_user_score_table
from sql_db.users import create_user_table, add_user, get_all_users, get_num_users
from sql_db.downloads import add_download, create_downloads_table, DownloadData, get_all_downloads, get_num_downloads
from sql_db.rankings import add_ranking, create_rankings_table, get_all_rankings, RankingData, get_num_rankings
from sql_db.images import add_image, create_image_table, get_all_images, ImageData, get_num_images
from utils.logging_utils import logger
from authlib.integrations.base_client import OAuthError
from fastapi import FastAPI, BackgroundTasks, Form, HTTPException, WebSocket, Cookie
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse, RedirectResponse
from starlette.requests import Request
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from aiocache import Cache
from aiocache.serializers import PickleSerializer
from aiocache.lock import RedLock
import tweepy

# DUMMY_IMG_URL = f"https://loremflickr.com/512/512"
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="!secret")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

config = Config('.env')
oauth = OAuth(config)

CONF_URL = 'https://accounts.google.com/.well-known/openid-configuration'
oauth.register(
    name='google',
    server_metadata_url=CONF_URL,
    client_kwargs={
        'scope': 'openid email profile'
    }
)

BACKEND_URLS = json.loads(os.environ["BACKEND_URLS"])
app.backend_urls = BACKEND_URLS[:]
MAX_SIZE_IN_QUEUE = len(app.backend_urls) * 2
MAX_SIZE_CONCURRENT = len(app.backend_urls) * 1
logger.debug(f"{MAX_SIZE_IN_QUEUE=} {MAX_SIZE_CONCURRENT=}")

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
BUCKET_NAME = "text-to-image-human-preferences"
S3_EXTRA_ARGS = {'ACL': 'public-read'}

REDIS_URL = os.environ.get("REDIS_URL")
url = urlparse(REDIS_URL)

consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret_key = os.environ['TWITTER_CONSUMER_SECRET_KEY']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']

twitter_auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
twitter_auth.set_access_token(access_token, access_token_secret)
twitter_api = tweepy.API(twitter_auth)

app.cache = Cache(Cache.REDIS, serializer=PickleSerializer(), namespace="main", endpoint=url.hostname, port=url.port,
                  password=url.password, timeout=0)
job_id2images = {}
job_id2images_data = {}
finished_job_id2uids = {}
scheduler = BackgroundScheduler()
BLOCKED_IDS = [641]


class UpdateImageRequest(BaseModel):
    image_uid: str
    prompt: str
    image_uids: List[str]


class TweetRequest(BaseModel):
    image_uid: str
    prompt: str
    image_data: str
    user_id: str


class Job(BaseModel):
    prompt: str
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "queued"
    start_time: int = Field(default_factory=lambda: time.time())
    image_uids: list = []
    progress: int = 0
    user_id: str = None

    def __str__(self):
        return f"Job(job_id={self.job_id}, status={self.status}, start_time={self.start_time}, image_uids={self.image_uids}, progress={self.progress}, user_id={self.user_id})"


async def get_job(job_id: str) -> Job:
    job = await app.cache.get(job_id)
    return job


async def set_job(job_id: str, job: Job):
    await app.cache.set(job_id, job)


async def clean_job(job_id):
    await app.cache.delete(job_id)


def is_user_logged(request):
    return "user_id" in request.session


@app.get('/')
async def homepage(request: Request):
    user = request.session.get('user')
    user_id = "null"
    user_score = 0
    if user:
        user_id = add_user(user["email"], user["name"])
        start = time.time()
        user_score = get_user_score(user_id)
        print(f"get_num_rankings_for_user took {time.time() - start:.2f} seconds {user_score=}")
        request.session['user_id'] = user_id
    return templates.TemplateResponse("index.html",
                                      {"request": request,
                                       "is_authenticated": is_user_logged(request),
                                       "user_id": user_id,
                                       "user_score": user_score})


@app.get('/login')
async def login(request: Request):
    redirect_uri = request.url_for('auth')
    return await oauth.google.authorize_redirect(request, redirect_uri)


@app.get('/auth')
async def auth(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
    except OAuthError as error:
        return HTMLResponse(f'<h1>{error.error}</h1>')
    user = token.get('userinfo')
    if user:
        request.session['user'] = dict(user)
        add_user(user["email"], user["name"])
    return RedirectResponse(url='/')


@app.get('/logout')
async def logout(request: Request):
    request.session.pop('user', None)
    request.session.pop('user_id', None)
    return RedirectResponse(url='/')


# async def get_random_images(job):
#     job.status = "running"
#     logger.debug(f"Getting random images for {job.job_id} with prompt {job.prompt}")
#     await asyncio.sleep(10)
#     images = []
#     for _ in range(4):
#         response = requests.get(DUMMY_IMG_URL)
#         image = Image.open(BytesIO(response.content))
#         buf = BytesIO()
#         image.save(buf, format='JPEG')
#         # Encode the image data as a base64-encoded string
#         image_data = base64.b64encode(buf.getvalue()).decode('utf-8')
#         images.append(image_data)
#     logger.debug(f"Got random images for {job.job_id} with prompt {job.prompt}")
#     job.status = "finished"
#     job.images = images
#     job.image_uids = [str(uuid.uuid4()) for _ in range(4)]


def upload_images(images, image_uids):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    for image, image_uid in zip(images, image_uids):
        pil_image = Image.open(BytesIO(base64.b64decode(image)))
        image_dir = "images"
        os.makedirs(image_dir, exist_ok=True)
        path = f"{image_dir}/{image_uid}.png"
        pil_image.save(path)
        s3_client.upload_file(path,
                              BUCKET_NAME,
                              path,
                              ExtraArgs=S3_EXTRA_ARGS)
        os.remove(path)


def extract_image_data(response_json, image_uids):
    image_data = []
    for i in range(len(response_json["prompt"])):
        image_data.append(
            ImageData(
                image_uid=image_uids[i],
                user_id=response_json["user_id"],
                prompt=response_json["prompt"][i],
                negative_prompt=response_json["negative_prompt"][i],
                seed=response_json["seed"],
                gs=response_json["gs"],
                steps=response_json["steps"],
                idx=response_json["idx"][i],
                num_generated=response_json["num_generated"],
                scheduler_cls=response_json["scheduler_cls"],
                model_id=response_json["model_id"]
            )
        )
    return image_data


async def get_backend_url_idx():
    async with RedLock(app.cache, "backend_url_idx", 1000):
        result = await app.cache.get("backend_url_idx")
        print(f"{result=}")
        await app.cache.set("backend_url_idx", result + 1)
    return result % len(app.backend_urls)


async def get_verified_backend_url(prompt):
    verified = False
    backend_url = None
    while not verified:
        backend_url_idx = await get_backend_url_idx()
        backend_url = app.backend_urls[backend_url_idx]
        try:
            response = requests.get(backend_url.replace("generate", ""), timeout=1.5)
            if response.status_code == 200:
                verified = True
            logger.debug(f"{backend_url=} {prompt=} worked")
        except Exception as e:
            app.backend_urls.remove(backend_url)
            logger.debug(f"{backend_url=} {prompt=} failed with exception {e}")
            continue
    return backend_url


def remove_square_brackets(prompt: str) -> Tuple[str, Optional[str]]:
    match = re.search(r'\[(.+?)\]', prompt)
    if match:
        return prompt.replace(match.group(), ""), match.group(1)
    return prompt.strip(), None


async def create_images(prompt, user_id):
    prompt, negative_prompt = remove_square_brackets(prompt)
    if negative_prompt is None:
        negative_prompt = "ugly, tiling, poorly drawn hands, poorly drawn feet, poorly drawn face, out of frame, mutation, mutated, extra limbs, extra legs, extra arms, disfigured, deformed, cross-eye, body out of frame, blurry, bad art, bad anatomy, blurred, text, watermark, grainy"

    num_samples = 4

    start = time.time()

    logger.info(f"Starting to create images for prompt {prompt} {os.getpid()=}")
    async with aiohttp.ClientSession() as session:
        has_generated = False
        num_tries = 0
        backend_url = await get_verified_backend_url(prompt)
        while not has_generated:
            try:
                async with session.post(backend_url,
                                        json={
                                            "prompt": prompt,
                                            "negative_prompt": negative_prompt,
                                            "num_samples": num_samples,
                                            "user_id": user_id
                                        }) as response:
                    response_json = await response.json()
                    has_generated = True
            except Exception as e:
                await asyncio.sleep(1)
                num_tries += 1
                logger.error(f"Error #{num_tries} creating images for prompt {prompt} with exception {e}")
                if num_tries > 5:
                    return None

    logger.info(f"Generating images from prompt {prompt} took {time.time() - start:.2f} seconds")
    images = response_json.pop("images")
    image_uids = [str(uuid.uuid4()) for _ in range(len(images))]
    image_data = extract_image_data(response_json, image_uids)
    return images[:num_samples], image_uids[:num_samples], image_data[:num_samples]


async def get_stable_images(job):
    job.status = "running"
    await set_job(job.job_id, job)
    result = await create_images(job.prompt, job.user_id)
    if result is None:
        job.status = "failed"
        await set_job(job.job_id, job)
    else:
        job_id2images[job.job_id], job.image_uids, job_id2images_data[job.job_id] = result
        job.status = "finished"
        await set_job(job.job_id, job)


async def consumer():
    # wait for service and update that we use it
    can_go_in = False
    while not can_go_in:
        async with RedLock(app.cache, "num_running", 1000):
            num_running = await app.cache.get("num_running")
            if num_running < MAX_SIZE_CONCURRENT:
                num_running += 1
                await app.cache.set("num_running", num_running)
                can_go_in = True
            await asyncio.sleep(0.5)
    logger.debug(f"{num_running=}")
    # reduce the size of the queue
    async with RedLock(app.cache, "qsize", 1000):
        qsize = await app.cache.get("qsize")
        qsize -= 1
        await app.cache.set("qsize", qsize)
        queue = await app.cache.get("queue")
        job_id = queue.popleft()
        await app.cache.set("queue", queue)
        logger.debug(f"{queue=} {qsize=}")

    # run the job
    job = await get_job(job_id)
    job.start_time = time.time()
    await set_job(job_id, job)
    # await get_random_images(job)
    logger.debug(f"Starting job {job.prompt}")
    await get_stable_images(job)
    logger.debug(f"Finished job {job.prompt}")

    # update num running
    async with RedLock(app.cache, "num_running", 1000):
        num_running = await app.cache.get("num_running")
        await app.cache.set("num_running", num_running - 1)


async def handle_images_request(prompt: str, user_id: str):
    async with RedLock(app.cache, f"qsize", 1000):
        qsize = await app.cache.get("qsize")
        logger.debug(f"handling request {qsize=}")
        if qsize >= MAX_SIZE_IN_QUEUE:
            return None
        job = Job(prompt=prompt, user_id=user_id)
        await set_job(job.job_id, job)
        await app.cache.set("qsize", qsize + 1)
        queue = await app.cache.get("queue")
        queue.append(job.job_id)
        await app.cache.set("queue", queue)
    return job.job_id


@app.websocket("/ws")
async def get_images(websocket: WebSocket):
    await websocket.accept()
    json_data = await websocket.receive_json()
    logger.debug(f"creating job for {json_data=}")
    user_id, prompt = json_data["user_id"], json_data["prompt"]
    job_id = await handle_images_request(prompt, user_id)
    if job_id is None or user_id in BLOCKED_IDS:
        await websocket.send_json({"status": "error"})
    else:
        asyncio.create_task(consumer())
        is_finished = False
        while not is_finished:
            job = await get_job(job_id)
            is_finished = job.status == "finished"
            elapsed_time = time.time() - job.start_time
            estimated_time = await app.cache.get("estimated_running_time")
            progress_text = f"Processing |"
            if job.status == "queued":
                queue = await app.cache.get("queue")
                queue_idx = queue.index(job_id)
                queue_real_position = (queue_idx // MAX_SIZE_CONCURRENT) + 1
                estimated_time = estimated_time * queue_real_position
                progress_text = f"Queue position: {queue_idx + 1}/{len(queue)} |"
            reported_estimated_time = estimated_time * 1.5
            progress_text += f" {round(elapsed_time, 1)}/{round(reported_estimated_time, 1)}s"
            job.progress = int(elapsed_time * 100 / reported_estimated_time) % 101
            message = {"status": job.status, "progress": job.progress, "progress_text": progress_text}
            if job.status in ["running", "queued"]:
                await websocket.send_json(message)
                await asyncio.sleep(0.5)
            elif job.status == "failed":
                logger.info(f"Job {job_id} failed")
                await websocket.send_json({"status": "failed"})
            else:
                print(job)
                await websocket.send_json(message)
                message["images"] = job_id2images[job_id]
                message["image_uids"] = job.image_uids
                await websocket.send_json(message)
                await set_job(job_id, job)
                await app.cache.set("estimated_running_time", 0.5 * elapsed_time + 0.5 * estimated_time)
                logger.debug(f"estimated running time {0.5 * elapsed_time + 0.5 * estimated_time:.2f}")
                finished_job_id2uids[job.job_id] = job.image_uids
    await websocket.close()


@app.post("/tweet/")
async def tweet_images(tweet: TweetRequest, request: Request):
    user_id = request.session.get('user_id')
    if not user_id:
        return RedirectResponse(url='/')

    image_uid = tweet.image_uid
    prompt = tweet.prompt
    image_data = tweet.image_data
    user_id = tweet.user_id
    logger.debug(f"TWEET - inside tweet images")
    image = Image.open(BytesIO(base64.b64decode(image_data)))
    os.makedirs(f"images", exist_ok=True)
    image.save(f"images/{image_uid}.png")
    tweet_text = f"{prompt} https://pickapic.io/ Join the effort!"
    logger.debug(f"tweeting {tweet_text=}")
    logger.debug(f"TWEET - before tweeting")
    status = twitter_api.update_status_with_media(tweet_text, f"images/{image_uid}.png")
    logger.debug(f"TWEET - after tweeting")
    os.remove(f"images/{image_uid}.png")
    tweet_text = f"{status.text} %23PickaPic %40PickaPicTweet"
    tweet_text = f"{tweet_text.replace(' ', '+')}"
    logger.debug(f"TWEET - returning text")
    return {"status": "ok", "tweet_text": tweet_text}


@app.post("/update_clicked_image/")
async def update_clicked_image(data: UpdateImageRequest, background_tasks: BackgroundTasks, request: Request):
    user_id = request.session.get('user_id')
    if not user_id:
        return RedirectResponse(url='/')

    image_uids = data.image_uids
    ranking_data = RankingData(
        user_id=user_id,
        image_0_uid=image_uids[0],
        image_1_uid=image_uids[1],
        image_2_uid=image_uids[2],
        image_3_uid=image_uids[3],
        best_image_uid=data.image_uid,
        prompt=data.prompt,
    )
    background_tasks.add_task(add_ranking, ranking_data)
    background_tasks.add_task(increment_user_score, user_id)
    logger.debug(f"Clicked on {data.image_uid} from {image_uids} with prompt {data.prompt}")
    return "success"


@app.post("/update_download_image/")
async def update_download_image(request: Request, data: UpdateImageRequest, background_tasks: BackgroundTasks):
    user_id = request.session.get('user_id')
    if not user_id:
        return RedirectResponse(url='/')
    image_uid = data.image_uid
    download_data = DownloadData(user_id, image_uid, data.prompt)
    background_tasks.add_task(add_download, download_data)
    logger.debug(f"Downloaded {image_uid}")
    return "success"


def update_urls():
    working_urls = []
    bad_urls = []
    for backend_url in BACKEND_URLS:
        try:
            response = requests.get(backend_url.replace("generate", ""))
            if response.status_code == 200:
                working_urls.append(backend_url)
        except Exception as e:
            bad_urls.append(backend_url)
            # logger.debug(f"{backend_url=} failed with exception {e}")
    app.backend_urls = working_urls
    if len(working_urls) < len(BACKEND_URLS):
        logger.debug(
            f"Updated: {len(app.backend_urls)}/{len(BACKEND_URLS)}\nWorking URLs: {app.backend_urls}\nBad URLs: {bad_urls}")


def clean_jobs():
    num_cleaned = 0
    job_ids = list(finished_job_id2uids.keys())
    for job_id in job_ids:
        if job_id not in finished_job_id2uids or job_id not in job_id2images:
            continue
        uids = finished_job_id2uids[job_id]
        user_id = job_id2images_data[job_id][0].user_id
        images = job_id2images[job_id]
        if user_id is not None:
            upload_images(images, uids)
        if job_id not in finished_job_id2uids or job_id not in job_id2images:
            continue
        del job_id2images[job_id]
        if user_id is not None:
            for image_data in job_id2images_data[job_id]:
                add_image(image_data)
        del job_id2images_data[job_id]
        del finished_job_id2uids[job_id]
        num_cleaned += 1
    if len(job_ids) > 0:
        logger.debug(f"Cleaned {num_cleaned}/{len(job_ids)} jobs")


def create_background_tasks():
    scheduler = BackgroundScheduler({'apscheduler.job_defaults.max_instances': 2})
    scheduler.add_job(func=update_urls, trigger="interval", seconds=180)
    scheduler.add_job(func=clean_jobs, trigger="interval", seconds=60)
    scheduler.start()


@app.on_event("startup")
async def startapp():
    create_user_table()
    create_image_table()
    create_rankings_table()
    create_downloads_table()
    create_user_score_table()
    create_background_tasks()
    await app.cache.set("backend_url_idx", 0)
    await app.cache.set("num_running", 0)
    await app.cache.set("qsize", 0)
    await app.cache.set("queue", collections.deque())
    await app.cache.set("estimated_running_time", 30)


@app.get('/metrics')
async def downloads(request: Request):
    user_id = request.session.get('user_id')
    if not user_id:
        return RedirectResponse(url='/')
    num_downloads = get_num_downloads()
    num_rankings = get_num_rankings()
    num_users = get_num_users()
    num_images = get_num_images()
    return {
        "num_downloads": num_downloads,
        "num_rankings": num_rankings,
        "num_users": num_users,
        "num_images": num_images,
    }
