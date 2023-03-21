import asyncio
import base64
import collections
import json
import os
import random
import re
import time
import traceback
import uuid
from io import BytesIO
from typing import List, Union, Tuple, Optional
from urllib.parse import urlparse
from fastapi_utils.tasks import repeat_every
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
from starlette_discord import DiscordOAuthClient

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
MAX_SIZE_IN_QUEUE = len(app.backend_urls) * 1
MAX_SIZE_CONCURRENT = len(app.backend_urls) // 2
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

STABILITY_API_KEY = os.environ['STABILITY_API_KEY']
STABILITY_API_HOST = os.environ['STABILITY_API_HOST']
STABILITY_ENGINE_ID_1 = "stable-diffusion-xl-v2-2"
STABILITY_ENGINE_ID_2 = "stable-diffusion-xl-beta-v2-2-2"

twitter_auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
twitter_auth.set_access_token(access_token, access_token_secret)
twitter_api = tweepy.API(twitter_auth)

discord_client_id = os.environ['DISCORD_CLIENT_ID']
discord_client_secret = os.environ['DISCORD_CLIENT_SECRET']
redirect_uri = "https://pickapic.io/discord_auth"

discord_client = DiscordOAuthClient(discord_client_id, discord_client_secret, redirect_uri)

job_id2images = {}
job_id2images_data = {}
finished_job_id2uids = {}
scheduler = BackgroundScheduler()
BLOCKED_IDS = [
    280, 331, 437, 641, 718, 729, 783, 984, 1023, 1040, 1059, 1149, 1187, 1177, 1202, 1203, 1220,
    1230, 1227, 1279, 1405, 1460, 1623, 1627, 1758, 1801, 1907, 1917, 1922, 2071, 2215, 2239, 2286, 2322, 2357, 2452,
    2459, 2481, 2513, 2515, 2520, 2545, 2596, 2603, 2617, 2638, 2709, 2783, 2842, 2266, 2899, 3084, 3138, 3243, 3264,
    3265, 3267, 3251, 3292, 3268, 3271, 1961, 3302, 3318, 1689, 3278, 1382, 3542, 3446, 3633, 1526
]
BLOCKED_IPS = []


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
    ip = request.client.host
    # logger.info(f"IP: {ip} {user=}")

    if ip in BLOCKED_IPS:
        logger.info(f"Blocking {ip=} {user=}")
        user = None
    user_id = "null"
    user_score = 0
    if user:
        user_id = add_user(user["email"], user["name"])
        start = time.time()
        user_score = get_user_score(user_id)
        print(f"user {user_id} logged in")
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


@app.get('/discord_login')
async def login(request: Request):
    redirect_uri = request.url_for('discord_auth')
    print(f"Discord {redirect_uri=}")
    discord_client.redirect_uri = redirect_uri
    return discord_client.redirect()


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


@app.get('/discord_auth')
async def discord_auth(code: str, request: Request):
    try:
        user = await discord_client.login(code)
    except OAuthError as error:
        return HTMLResponse(f'<h1>{error.error}</h1>')
    print(f"Discord {user=}")
    email = str(user.email if user.email else user.id)
    if user:
        request.session['user'] = {"email": email, "name": user.username}
        add_user(email, user.username)
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
        try:
            if os.path.exists(path):
                s3_client.upload_file(path,
                                      BUCKET_NAME,
                                      path,
                                      ExtraArgs=S3_EXTRA_ARGS)
            else:
                logger.warning(f"Couldn't upload image {image_uid} - path exists={os.path.exists(path)}")
        except Exception as e:
            logger.error(f"Couldn't upload image {image_uid} - path exists={os.path.exists(path)} - {e}")
        if os.path.exists(path):
            os.remove(path)


def extract_image_data(response_json, image_uids):
    image_data = []
    for i in range(len(response_json["prompt"])):
        image_data.append(
            ImageData(
                image_uid=image_uids[i],
                user_id=response_json["user_id"][i],
                prompt=response_json["prompt"][i],
                negative_prompt=response_json["negative_prompt"][i],
                seed=response_json["seed"][i],
                gs=response_json["gs"][i],
                steps=response_json["steps"][i],
                idx=response_json["idx"][i],
                num_generated=response_json["num_generated"][i],
                scheduler_cls=response_json["scheduler_cls"][i],
                model_id=response_json["model_id"][i]
            )
        )
    return image_data


async def get_backend_url_idx():
    async with RedLock(app.cache, "backend_url_idx", 1000):
        result = await app.cache.get("backend_url_idx")
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


async def generate_images(prompt, negative_prompt, num_samples, user_id, backend_url):
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        has_generated = False
        num_tries = 0
        while not has_generated:
            try:
                # logger.debug(f"calling {backend_url} with prompt {prompt}")
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
                logger.error(traceback.format_exc())
                if num_tries > 5:
                    return None
    logger.info(
        f"Generated {num_samples} images with {backend_url} for prompt {prompt} in {time.time() - start_time:.2f} seconds")
    return response_json


async def generate_images_via_api(prompt, negative_prompt, user_id, engine_id):
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        has_generated = False
        num_tries = 0
        seed = random.randint(0, 2147483647)
        gs = random.uniform(3, 12)
        asscore = random.choice([6.0, 6.25, 6.5, 6.75, 7.0, 7.25, 7.5, 7.75])
        height = 512
        width = 512
        n_steps = 25
        scheduler_cls = "K_DPMPP_2M"

        if "beta" in engine_id:
            prompt = "$IPCinline:{" + "\"sdxl_ascore\"" + f":[{asscore},2.5]" + "}$ " + prompt

        while not has_generated:
            try:
                async with session.post(
                        f"{STABILITY_API_HOST}/v1beta/generation/{engine_id}/text-to-image",
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "Authorization": f"Bearer {STABILITY_API_KEY}"
                        },
                        json={
                            "text_prompts": [
                                {
                                    "text": prompt,
                                    "weight": 1.0
                                },
                                {
                                    "text": negative_prompt,
                                    "weight": -1.0
                                },
                            ],
                            "cfg_scale": gs,
                            "height": height,
                            "width": width,
                            "sampler": scheduler_cls,
                            "samples": 1,
                            "steps": n_steps,
                            "seed": seed,
                        },
                ) as response:
                    data = await response.json()
                    image_bytes = [dp['base64'] for dp in data["artifacts"]]
                    was_filtered = any(dp['finishReason'] == "CONTENT_FILTERED" for dp in data["artifacts"])
                    if was_filtered:
                        logger.error(f"{user_id=} {prompt=} FILTERED!!!")
                    response_json = {
                        "user_id": user_id,
                        "prompt": [prompt],
                        "negative_prompt": [negative_prompt],
                        "seed": seed,
                        "gs": [gs],
                        "steps": n_steps,
                        "idx": [0],
                        "num_generated": 1,
                        "scheduler_cls": scheduler_cls,
                        "model_id": engine_id if not was_filtered else "CONTENT_FILTERED",
                        "images": image_bytes
                    }
                    has_generated = True
            except Exception as e:
                await asyncio.sleep(1)
                num_tries += 1
                logger.error(f"Error #{num_tries} creating images for prompt {prompt} with exception {e}")
                if num_tries > 5:
                    return None
    logger.info(f"Generated 1 images with {engine_id} for prompt {prompt} in {time.time() - start_time:.2f} seconds")
    return response_json


async def create_images(prompt, user_id):
    prompt, negative_prompt = remove_square_brackets(prompt)
    if negative_prompt is None:
        negative_prompt = "ugly, deformed, noisy, blurry, distorted, grainy"

    start = time.time()

    # logger.info(f"Starting: {prompt=} | time={time.time() - start:.2f}(sec) | {user_id=}")
    num_samples_per_call = 2
    backend_url1 = await get_verified_backend_url(prompt)
    tasks = []

    task1 = asyncio.create_task(generate_images(
        prompt=prompt,
        negative_prompt=negative_prompt,
        user_id=user_id,
        num_samples=num_samples_per_call,
        backend_url=backend_url1
    ))
    tasks.append(task1)

    task2 = asyncio.create_task(generate_images_via_api(
        prompt=prompt,
        negative_prompt=negative_prompt,
        user_id=user_id,
        engine_id=STABILITY_ENGINE_ID_1
    ))
    tasks.append(task2)

    task3 = asyncio.create_task(generate_images_via_api(
        prompt=prompt,
        negative_prompt=negative_prompt,
        user_id=user_id,
        engine_id=STABILITY_ENGINE_ID_1
    ))
    tasks.append(task3)

    task4 = asyncio.create_task(generate_images_via_api(
        prompt=prompt,
        negative_prompt=negative_prompt,
        user_id=user_id,
        engine_id=STABILITY_ENGINE_ID_2
    ))
    tasks.append(task4)

    task5 = asyncio.create_task(generate_images_via_api(
        prompt=prompt,
        negative_prompt=negative_prompt,
        user_id=user_id,
        engine_id=STABILITY_ENGINE_ID_2
    ))
    tasks.append(task5)

    for task in tasks:
        await task

    responses = [task.result() for task in tasks]
    total_response_json = collections.defaultdict(list)
    for key in responses[0]:
        for i, response in enumerate(responses):
            if isinstance(responses[0][key], list):
                total_response_json[key] += response[key]
            else:
                total_response_json[key] += [response[key]] * (num_samples_per_call if i == 0 else 1)

    user_score = get_user_score(user_id)
    logger.info(
        f"Generation: {prompt=} | time={time.time() - start:.2f}(sec) | {user_id=} | {os.getpid()=} | {user_score=} | {backend_url1=}")
    images = total_response_json.pop("images")
    image_uids = [str(uuid.uuid4()) for _ in range(len(images))]
    image_data = extract_image_data(total_response_json, image_uids)
    return images, image_uids, image_data


async def get_stable_images(job):
    job.status = "running"
    await set_job(job.job_id, job)
    result = await create_images(job.prompt, job.user_id)
    if result is None:
        job.status = "failed"
        await set_job(job.job_id, job)
    else:
        job_id2images[job.job_id], job.image_uids, job_id2images_data[job.job_id] = result
        finished_job_id2uids[job.job_id] = job.image_uids
        # user_score = get_user_score(job.user_id)
        # logger.debug(
        #     f"Finished: {job.prompt=} | {job.user_id=} | {job.job_id=} | {job.job_id in job_id2images} | {os.getpid()=} | {user_score=}")
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
    logger.debug(f"num_running {num_running}/{MAX_SIZE_CONCURRENT}")
    # reduce the size of the queue
    should_run = True
    async with RedLock(app.cache, "qsize", 1000):
        queue = await app.cache.get("queue")
        if len(queue) == 0:
            should_run = False
        else:
            job_id = queue.popleft()
        await app.cache.set("qsize", len(queue))
        await app.cache.set("queue", queue)
        logger.debug(f"queue {len(queue)}/{MAX_SIZE_IN_QUEUE}")

    # run the job
    if should_run:
        job = await get_job(job_id)
        job.start_time = time.time()
        await set_job(job_id, job)
        # await get_random_images(job)
        await get_stable_images(job)

    # update num running
    async with RedLock(app.cache, "num_running", 1000):
        num_running = await app.cache.get("num_running")
        await app.cache.set("num_running", max(num_running - 1, 0))


async def handle_images_request(prompt: str, user_id: str):
    async with RedLock(app.cache, f"qsize", 1000):
        qsize = await app.cache.get("qsize")
        logger.debug(f"handling request {qsize=}")
        if qsize >= MAX_SIZE_IN_QUEUE:
            return None
        job = Job(prompt=prompt, user_id=user_id)
        await set_job(job.job_id, job)
        queue = await app.cache.get("queue")
        queue.append(job.job_id)
        await app.cache.set("qsize", len(queue))
        await app.cache.set("queue", queue)
    return job.job_id


@app.websocket("/ws")
async def get_images(websocket: WebSocket):
    await websocket.accept()
    json_data = await websocket.receive_json()
    user_id, prompt = json_data["user_id"], json_data["prompt"]
    user_score = get_user_score(user_id)
    job_id = await handle_images_request(prompt, user_id)
    if job_id is None or user_id in BLOCKED_IDS or user_score > 3000:
        await websocket.send_json({"status": "error"})
    else:
        asyncio.create_task(consumer())
        asyncio.create_task(consumer())
        is_finished = False
        num_queued = 0
        while not is_finished:
            job = await get_job(job_id)
            is_finished = job.status in ["finished", "failed"]
            elapsed_time = time.time() - job.start_time
            estimated_time = await app.cache.get("estimated_running_time")
            progress_text = f"Generating |"
            if job.status == "queued":
                queue = await app.cache.get("queue")
                if job_id not in queue:
                    logger.warning(f"job {job} job_id {job_id} not in queue {queue}")
                    await asyncio.sleep(1)
                    num_queued += 1
                    if num_queued > 5:
                        job.status = "failed"
                        await set_job(job_id, job)
                    continue
                queue_idx = queue.index(job_id)
                queue_real_position = (queue_idx // MAX_SIZE_CONCURRENT) + 1
                estimated_time = estimated_time * queue_real_position
                progress_text = f"Queue position: {queue_idx + 1}/{len(queue)} |"
            reported_estimated_time = estimated_time * 1.5
            progress_text += f" {round(elapsed_time, 1)}/{round(reported_estimated_time, 1)}s"
            job.progress = int(elapsed_time * 100 / reported_estimated_time) % 101
            message = {"status": job.status, "progress": job.progress, "progress_text": progress_text}
            try:
                if job.status in ["running", "queued"]:
                    await websocket.send_json(message)
                    await asyncio.sleep(0.5)
                elif job.status == "failed" or job_id not in job_id2images:
                    logger.error(
                        f"Job {job} {job_id} failed - {job_id} in job_id2images = {job_id in job_id2images} | {os.getpid()=}")
                    await websocket.send_json({"status": "failed"})
                else:
                    # print(job)
                    await websocket.send_json(message)
                    message["images"] = job_id2images[job_id]
                    message["image_uids"] = job.image_uids
                    await websocket.send_json(message)
                    await set_job(job_id, job)
                    await app.cache.set("estimated_running_time", 0.5 * elapsed_time + 0.5 * estimated_time)
                    # logger.debug(f"estimated running time {0.5 * elapsed_time + 0.5 * estimated_time:.2f}")
            except:
                logger.error(f"Failed to send message {message}")
                logger.error(traceback.format_exc())
                break
    if job_id is not None:
        await clean_job(job_id)
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
    tweet_text = f"""{prompt} 
https://pickapic.io/
Generate cool images for free and contribute to open science!"""
    logger.debug(f"tweeting {tweet_text=}")
    logger.debug(f"TWEET - before tweeting {tweet_text=}")
    status = twitter_api.update_status_with_media(tweet_text, f"images/{image_uid}.png")
    logger.debug(f"TWEET - after tweeting")
    image_path = f"images/{image_uid}.png"
    if os.path.exists(image_path):
        os.remove(image_path)
    tweet_text = f"{status.text}\n %23PickaPic\n %40PickaPicTweet"
    tweet_text = tweet_text.replace(' ', '+').replace('\n', '%0A')
    logger.debug(f"TWEET - returning text - {tweet_text=}")
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
        image_2_uid=image_uids[2] if len(image_uids) > 2 else None,
        image_3_uid=image_uids[3] if len(image_uids) > 3 else None,
        best_image_uid=data.image_uid,
        prompt=data.prompt,
    )
    background_tasks.add_task(add_ranking, ranking_data)
    background_tasks.add_task(increment_user_score, user_id)
    logger.debug(f"{user_id=} clicked image")
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
    job_ids = [job_id for job_id in finished_job_id2uids.keys() if job_id in job_id2images]
    time.sleep(15)
    for job_id in job_ids:
        if job_id not in finished_job_id2uids or job_id not in job_id2images:
            logger.warning(
                f"Cleaning 1: in finished_job_id2uids={job_id in finished_job_id2uids} or in job_id2images={job_id in job_id2images} for {job_id}")
            continue
        uids = finished_job_id2uids[job_id]
        user_id = job_id2images_data[job_id][0].user_id
        images = job_id2images[job_id]
        if user_id is not None:
            upload_images(images, uids)
        if job_id not in finished_job_id2uids or job_id not in job_id2images:
            logger.warning(
                f"Cleaning 2: in finished_job_id2uids={job_id in finished_job_id2uids} or in job_id2images={job_id in job_id2images} for {job_id}")
            continue
        del job_id2images[job_id]
        if user_id is not None:
            for image_data in job_id2images_data[job_id]:
                add_image(image_data)
        del job_id2images_data[job_id]
        del finished_job_id2uids[job_id]
        num_cleaned += 1
    if 0 < len(job_ids) != num_cleaned:
        logger.warning(f"Cleaned {num_cleaned}/{len(job_ids)} jobs")
    else:
        logger.debug(f"Cleaned {num_cleaned}/{len(job_ids)} jobs")


def create_background_tasks():
    scheduler = BackgroundScheduler({'apscheduler.job_defaults.max_instances': 2})
    scheduler.add_job(func=update_urls, trigger="interval", seconds=180)
    scheduler.add_job(func=clean_jobs, trigger="interval", seconds=120)
    scheduler.start()


@app.on_event("startup")
@repeat_every(seconds=60 * 15)
async def startapp():
    print("Starting app")
    app.cache = Cache(Cache.REDIS, serializer=PickleSerializer(), namespace="main", endpoint=url.hostname,
                      port=url.port,
                      password=url.password, timeout=0)
    async with RedLock(app.cache, "qsize", 1000):
        async with RedLock(app.cache, "num_running", 1000):
            create_user_table()
            create_image_table()
            create_rankings_table()
            create_downloads_table()
            create_user_score_table()
            create_background_tasks()
            global job_id2images, job_id2images_data, finished_job_id2uids
            job_id2images = {}
            job_id2images_data = {}
            finished_job_id2uids = {}
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
