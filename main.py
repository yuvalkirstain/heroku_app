import asyncio
import base64
import json
import os
import time
import uuid
from io import BytesIO
from typing import List

import aiohttp
import boto3
import requests
from PIL import Image
from pydantic import BaseModel, Field

from sql_db.users import create_user_table, add_user, get_all_users
from sql_db.downloads import add_download, create_downloads_table, DownloadData, get_all_downloads
from sql_db.rankings import add_ranking, create_rankings_table, get_all_rankings, RankingData
from sql_db.images import add_image, create_image_table, get_all_images, ImageData
from utils.logging_utils import logger
from authlib.integrations.base_client import OAuthError
from fastapi import FastAPI, BackgroundTasks, Form, HTTPException
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse, RedirectResponse
from starlette.requests import Request
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from collections import OrderedDict

logger.debug("importing demo")

logger.debug("importing DB")

DUMMY_IMG_URL = f"https://loremflickr.com/512/512"
logger.debug("finished importing DB")
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

logger.debug("Finished importing DB")

BACKEND_URLS = json.loads(os.environ["BACKEND_URLS"])
backend_url_idx = 0
MAX_SIZE_IN_QUEUE = len(BACKEND_URLS) * 2
MAX_SIZE_CONCURRENT = len(BACKEND_URLS) * 3
logger.debug(f"{MAX_SIZE_IN_QUEUE=} {MAX_SIZE_CONCURRENT=}")

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
BUCKET_NAME = "text-to-image-human-preferences"
S3_EXTRA_ARGS = {'ACL': 'public-read'}

app.backend_url_semaphore = asyncio.Semaphore(1)
app.job_adding_semaphore = asyncio.Semaphore(1)
app.semaphore = asyncio.Semaphore(MAX_SIZE_CONCURRENT)
app.queue = asyncio.Queue(maxsize=MAX_SIZE_IN_QUEUE)
app.jobs = OrderedDict()


class UpdateImageRequest(BaseModel):
    image_uid: str
    prompt: str
    image_uids: List[str]


class Job(BaseModel):
    prompt: str
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "queued"
    start_time: int = Field(default_factory=lambda: time.time())
    images: list = []
    image_uids: list = []
    estimated_total_time: int = 15
    progress: int = 0
    user_id: str = None
    image_data: List = None


def is_user_logged(request):
    return "user_id" in request.session


@app.get('/')
async def homepage(request: Request):
    user = request.session.get('user')
    if user:
        user_id = add_user(user["email"], user["name"])
        request.session['user_id'] = user_id
    return templates.TemplateResponse("index.html",
                                      {"request": request, "is_authenticated": is_user_logged(request)})


@app.get('/login')
async def login(request: Request):
    redirect_uri = request.url_for('auth')
    return await oauth.google.authorize_redirect(request, redirect_uri)


@app.get('/auth')
async def auth(request: Request):
    print("auth")
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


async def create_images(prompt, user_id):
    await app.backend_url_semaphore.acquire()
    global backend_url_idx

    verified = False
    backend_url_idx = (backend_url_idx + 1) % len(BACKEND_URLS)
    while not verified:
        backend_url_idx = backend_url_idx % len(BACKEND_URLS)
        backend_url = BACKEND_URLS[backend_url_idx]
        print(backend_url)
        try:
            response = requests.get(backend_url.replace("generate", ""), timeout=0.5)
            print(response.elapsed)
            if response.status_code == 200:
                verified = True
        except requests.exceptions.Timeout:
            BACKEND_URLS.remove(backend_url)
            continue
    app.backend_url_semaphore.release()

    negative_prompt = "ugly, tiling, poorly drawn hands, poorly drawn feet, poorly drawn face, out of frame, mutation, mutated, extra limbs, extra legs, extra arms, disfigured, deformed, cross-eye, body out of frame, blurry, bad art, bad anatomy, blurred, text, watermark, grainy"
    num_samples = 4

    start = time.time()

    print("Starting to create images")
    async with aiohttp.ClientSession() as session:
        async with session.post(backend_url,
                                json={
                                    "prompt": prompt,
                                    "negative_prompt": negative_prompt,
                                    "num_samples": num_samples,
                                    "user_id": user_id
                                }) as response:
            response_json = await response.json()

    print(f"Generating images took {time.time() - start:.2f} seconds")
    images = response_json.pop("images")
    image_uids = [str(uuid.uuid4()) for _ in range(len(images))]
    image_data = extract_image_data(response_json, image_uids)
    return images[:num_samples], image_uids[:num_samples], image_data[:num_samples]


async def get_stable_images(job):
    job.status = "running"
    job.images, job.image_uids, job.image_data = await create_images(job.prompt, job.user_id)
    job.status = "finished"


async def consumer():
    await app.semaphore.acquire()
    job = await app.queue.get()
    job.start_time = time.time()
    # await get_random_images(job)
    logger.debug(f"Starting job {job.prompt}")
    await get_stable_images(job)
    logger.debug(f"Finished job {job.prompt}")
    app.semaphore.release()
    app.queue.task_done()


@app.post("/get_images/")
async def get_images(prompt: str = Form(...),
                     request: Request = None,
                     background_tasks: BackgroundTasks = None):
    print("Inside get_images")
    user_id = request.session.get('user_id')
    if not user_id:
        return RedirectResponse(url='/')
    await app.job_adding_semaphore.acquire()
    if app.queue.qsize() >= MAX_SIZE_IN_QUEUE:
        app.job_adding_semaphore.release()
        raise HTTPException(status_code=429, detail="too many users")
    job = Job(prompt=prompt, user_id=user_id)
    app.jobs[job.job_id] = job
    print(list(app.jobs.keys()))
    await app.queue.put(job)
    asyncio.create_task(consumer())
    app.job_adding_semaphore.release()
    return {"jobId": job.job_id}


@app.get("/get_images_status/")
async def get_images_status(job_id: str):
    job = app.jobs[job_id]
    if job.status in ["running", "queued"]:
        elapsed_time = time.time() - job.start_time
        job.progress = int(elapsed_time * 100 / job.estimated_total_time) % 101
    return job


@app.get("/get_images_result/")
async def get_images_result(job_id: str, background_tasks: BackgroundTasks = None, request: Request = None):
    user_id = request.session.get('user_id')
    if not user_id:
        return RedirectResponse(url='/')
    job = app.jobs[job_id]
    background_tasks.add_task(upload_images, job.images, job.image_uids)
    for image_data in job.image_data:
        background_tasks.add_task(add_image, image_data)
    return job


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


@app.on_event("startup")
async def startapp():
    logger.debug("Init DB")
    create_user_table()
    create_image_table()
    create_rankings_table()
    create_downloads_table()
    logger.debug("Finished Init DB")


@app.get('/users')
async def users(request: Request):
    user_id = request.session.get('user_id')
    if not user_id or user_id != 1:
        return RedirectResponse(url='/')
    users = get_all_users()
    return HTMLResponse(users.to_html())


@app.get('/images')
async def images():
    images = get_all_images()
    return HTMLResponse(images.to_html())


@app.get('/rankings')
async def rankings():
    rankings = get_all_rankings()
    return HTMLResponse(rankings.to_html())


@app.get('/downloads')
async def downloads():
    downloads = get_all_downloads()
    return HTMLResponse(downloads.to_html())
