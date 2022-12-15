from utils import logger

from authlib.integrations.base_client import OAuthError
from fastapi import FastAPI

from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse, RedirectResponse
from starlette.requests import Request
import gradio as gr
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
logger.debug("importing demo")
from gr_app import demo
logger.debug("importing DB")
from sql_db import add_user, create_user_table, create_image_table, create_rankings_table, get_all_users, get_all_images, get_all_rankings # download_db

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


@app.get('/')
async def homepage(request: Request):
    user = request.session.get('user')
    if user:
        add_user(user["email"], user["name"])
        return RedirectResponse("/legal")
    return templates.TemplateResponse("home.html", {"request": request})


@app.get('/legal')
async def legal(request: Request):
    user = request.session.get('user')
    logger.debug(f"{user}")
    if user:
        add_user(user["email"], user["name"])
        return templates.TemplateResponse("legal.html", {"request": request})
    return RedirectResponse("/")


@app.get('/info')
async def legal(request: Request):
    return templates.TemplateResponse("info.html",
                                      {"request": request, "is_authenticated": request.session.get('user') is not None})


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
    return RedirectResponse(url='/')


@app.on_event("startup")
async def create_db():
    logger.debug("Init DB")
    # download_db()
    create_user_table()
    create_image_table()
    create_rankings_table()
    logger.debug("Finished Init DB")


@app.get('/users')
async def users():
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


logger.debug("Mounting gradio app")
gradio_app = gr.mount_gradio_app(app, demo, "/gradio")
