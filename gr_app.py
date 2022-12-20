import base64
import json
import os
from datetime import datetime

from utils import logger
from dataclasses import asdict

import gradio as gr
import requests
from PIL import Image
from io import BytesIO

logger.debug("Importing db")
from sql_db import IMAGE_DIR, ImageData, RankingData, add_ranking, get_user_by_email, add_image

logger.debug("Finished importing db")
BACKEND_URLS = json.loads(os.environ["BACKEND_URLS"])
logger.debug(f"BACKEND_URLS={BACKEND_URLS}")
DUMMY_IMG_URL = f"https://loremflickr.com/256/256"
IMAGES_DATA_NAME = "images_data"
IMAGES_NAME = "images"
BEST_IMAGE_IDX_NAME = 'best_image_idx'

cur_backend_url_idx = 0

def get_random_images():
    logger.debug("Getting random images")
    images = []
    for _ in range(4):
        response = requests.get(DUMMY_IMG_URL)
        images.append(Image.open(BytesIO(response.content)))
    logger.debug("Got random images")
    return images


def get_stable_images(prompt, negative_prompt, num_samples, user_id):
    logger.debug("Generating images...")
    start = datetime.now()
    global cur_backend_url_idx
    print(f"Using backend {BACKEND_URLS[cur_backend_url_idx]}")
    backend_url = BACKEND_URLS[cur_backend_url_idx]
    cur_backend_url_idx = (cur_backend_url_idx + 1) % len(BACKEND_URLS)
    response = requests.post(
        backend_url,
        json={
            "prompt": prompt,
            "negative_prompt": negative_prompt,
            "user_id": user_id,
            "num_samples": num_samples,
        }
    )
    print(f"Generating images took {datetime.now() - start}")
    start = datetime.now()
    response_json = response.json()
    images = response_json.pop("images")
    pil_images = []
    for image in images[:num_samples]:
        image = Image.open(BytesIO(base64.b64decode(image)))
        pil_images.append(image)
    print(f"Decoding images took {datetime.now() - start}")
    image_data = []
    for i in range(num_samples):
        image_data.append(ImageData(user_id=response_json["user_id"],
                                    prompt=response_json["prompt"][i],
                                    negative_prompt=response_json["negative_prompt"][i],
                                    seed=response_json["seed"],
                                    gs=response_json["gs"],
                                    steps=response_json["steps"],
                                    idx=response_json["idx"][i],
                                    num_generated=response_json["num_generated"],
                                    scheduler_cls=response_json["scheduler_cls"],
                                    model_id=response_json["model_id"]))
    logger.debug("Finished generating images...")
    return pil_images, image_data


def run_model(prompt, state, request: gr.Request):
    negative_prompt = "ugly, tiling, poorly drawn hands, poorly drawn feet, poorly drawn face, out of frame, mutation, mutated, extra limbs, extra legs, extra arms, disfigured, deformed, cross-eye, body out of frame, blurry, bad art, bad anatomy, blurred, text, watermark, grainy"
    user_mail = state["user_mail"]
    best_image_update = gr.update(visible=True, interactive=True, value=None)
    user_id = get_user_by_email(user_mail).user_id
    num_samples = 4 if state[BEST_IMAGE_IDX_NAME] is None else 3
    images, images_data = get_stable_images(prompt, negative_prompt, num_samples, user_id)
    best_image_idx = state[BEST_IMAGE_IDX_NAME]
    if best_image_idx is not None:
        images_data.insert(best_image_idx, ImageData(**state[IMAGES_DATA_NAME][best_image_idx]))
        images.insert(best_image_idx, state[IMAGES_NAME][best_image_idx])
    start = datetime.now()
    for image, image_data in zip(images, images_data):
        image_hash = hash(image_data)
        add_image(image_data)
        image.save(f"{IMAGE_DIR}/{image_hash}.png")
    print(f"Saving images took {datetime.now() - start}")
    state[IMAGES_DATA_NAME] = [asdict(image_data) for image_data in images_data]
    state[IMAGES_NAME] = images
    image_captions = [(image, f"Generated image {i + 1}") for i, image in enumerate(images)]
    gallery_update = gr.update(visible=True, value=image_captions)
    run = gr.update(visible=False)
    download = gr.update(visible=False)
    return best_image_update, gallery_update, run, state, download


def make_demo_visible(state, request: gr.Request):
    for k in state:
        state[k] = None
    print(request.kwargs)
    if request.request.session.get('user'):
        prompt = gr.update(visible=True, interactive=True)
        clear_btn = gr.update(visible=True)
        submit_btn = gr.update(visible=True)
        # negative_prompt = gr.update(visible=False, interactive=True)  # TODO do we want to make it visible?
        start_btn = gr.update(visible=False)
        state["user_mail"] = request.request.session.get('user')['email']
    else:
        prompt = gr.update(visible=False, interactive=True)
        clear_btn = gr.update(visible=False)
        submit_btn = gr.update(visible=False)
        # negative_prompt = gr.update(visible=False, interactive=True)
        start_btn = gr.update(visible=False)
    # return prompt, clear_btn, submit_btn, negative_prompt, start_btn, state
    return prompt, clear_btn, submit_btn, start_btn, state



def best_image_click(best_image, state):
    if best_image is None:
        best_image = gr.update(interactive=True)
        run = gr.update()
        gallery = gr.update()
        download = gr.update(visible=False)
    else:
        best_image_idx = int(best_image.split(" ")[-1]) - 1
        # TODO we want to upload the ranking
        images_data = state[IMAGES_DATA_NAME]
        image_hashes = [hash(ImageData(**image_data)) for image_data in images_data]
        data = {f"image_{i}_hash": image_hashes[i] for i in range(len(image_hashes))}
        data["best_image_hash"] = image_hashes[best_image_idx]
        data["user_id"] = get_user_by_email(state["user_mail"]).user_id
        ranking = RankingData(**data)
        add_ranking(ranking)
        state[BEST_IMAGE_IDX_NAME] = best_image_idx
        best_image = gr.update(visible=False)
        run = gr.update(visible=True)
        images = state[IMAGES_NAME]
        black = Image.new("RGB", images[0].size, color=0)
        images = [Image.blend(image, black, 0.7) if i != best_image_idx else image for i, image in enumerate(images)]
        image_captions = [(image, f"Generated image {i + 1}") for i, image in enumerate(images)]
        gallery = gr.update(visible=True, value=image_captions)
        download = prepare_img(state)
    return best_image, run, state, gallery, download


def clear_all(state):
    for k in state:
        if k == "user_mail":
            continue
        state[k] = None

    prompt = gr.update(visible=True, interactive=True, value="")
    gallery = gr.update(visible=False, value=[])
    best_image = gr.update(visible=False, value=None)
    run = gr.update(value="Submit prompt", visible=True)
    download = gr.update(visible=False)
    return prompt, gallery, best_image, run, state, download


def submit_prompt():
    prompt = gr.update(visible=True, interactive=False)
    gallery = gr.update(visible=True)
    run_btn = gr.update(value="Generate more images", visible=False)
    return prompt, gallery, run_btn

def prepare_img(state):
    image_data = state[IMAGES_DATA_NAME][state[BEST_IMAGE_IDX_NAME]]
    image_hash = hash(ImageData(**image_data))
    download = gr.update(value=f"{IMAGE_DIR}/{image_hash}.png", visible=True)
    return download


logger.debug("Starting to create demo")

with gr.Blocks(css=""".gradio-container {
background-image: 
    linear-gradient(
      rgba(0, 0, 0, 0.8),
      rgba(0, 0, 0, 0.8)
    ),
    url('https://images.squarespace-cdn.com/content/v1/6213c340453c3f502425776e/a9dbb9ff-a0bc-4a02-b2bb-cf69b7bfc350/SDv2-1_artstyles.jpg');
    min-height: 100%;
    background-position: center;
    background-size: cover;
}
h1.with-eight {
    text-shadow:
        0.05em 0 black,
        0 0.05em black,
        -0.05em 0 black,
        0 -0.05em black,
        -0.05em -0.05em black,
        -0.05em 0.05em black,
        0.05em -0.05em black,
        0.05em 0.05em black;
}
""") as demo:
    gr.HTML(
        """<h1 class="with-eight" align="center" style="font-size:30px; font-weight:bold; color:white"> Pick a Pic</h1>""")
    start_btn = gr.Button("Press here to start!")

    state = gr.State(
        value={
            "prompt": None,
            BEST_IMAGE_IDX_NAME: None,
            IMAGES_DATA_NAME: None,
            IMAGES_NAME: None,
            "user_mail": None,
        }
    )

    with gr.Column():
        prompt = gr.Textbox(
            label="Model Prompt",
            placeholder="Write here a description of an image (prompt/caption) and press Submit Prompt.",
            visible=False,
            lines=2
        )

        with gr.Row():
            clear_btn = gr.Button(
                "Clear Prompt",
                visible=False
            )

            run_btn = gr.Button(
                "Submit Prompt",
                visible=False,
                variant="primary"
            )

        # negative_prompt = gr.Textbox(
        #     label="Negative Model Prompt",
        #     interactive=True,
        #     value="ugly, tiling, poorly drawn hands, poorly drawn feet, poorly drawn face, out of frame, mutation, mutated, extra limbs, extra legs, extra arms, disfigured, deformed, cross-eye, body out of frame, blurry, bad art, bad anatomy, blurred, text, watermark, grainy",
        #     placeholder="Write here a negative prompt.",
        #     visible=False,
        #     lines=2
        # )

    with gr.Column():
        best_image = gr.Radio(
            [f"Generated image {i + 1}" for i in range(4)],
            label="Which image is the best?",
            elem_id="best-image",
            visible=False
        )

        download = gr.File(label="Download Image", visible=False)
        gallery = gr.Gallery(
            label="Generated images",
            show_label=False,
            elem_id="gallery",
            visible=False,
            value=None  # [DUMMY_IMG_URL] * 4
        ).style(grid=[2], height="auto")


    start_btn.click(
        make_demo_visible,
        queue=False,
        inputs=[state],
        # outputs=[prompt, clear_btn, run_btn, negative_prompt, start_btn, state]
        outputs=[prompt, clear_btn, run_btn, start_btn, state]
    )

    # After the user submits the prompt, we allow them to run the model.
    prompt.submit(
        submit_prompt,
        queue=False,
        outputs=[prompt, gallery, run_btn]
    )
    run_btn.click(
        submit_prompt,
        queue=False,
        outputs=[prompt, gallery, run_btn]
    )

    # When the user runs the model, we show the generated images and best image radio.
    run_btn.click(
        run_model,
        inputs=[prompt, state],
        # inputs=[prompt, negative_prompt, state],
        outputs=[best_image, gallery, run_btn, state, download]
    )

    # After the user chooses the best image they can run the model again.
    best_image.change(
        best_image_click,
        queue=False,
        inputs=[best_image, state],
        outputs=[best_image, run_btn, state, gallery, download],
    )

    # If the user wants to change prompt, we clear everything
    clear_btn.click(
        clear_all,
        queue=False,
        inputs=[state],
        outputs=[prompt, gallery, best_image, run_btn, state, download]
    )

concurrency_count = len(BACKEND_URLS) * 8
max_size = len(BACKEND_URLS) * 8 * 4
logger.debug(f"Finished importing demo:\n   concurrency_count: {concurrency_count}\n    max_size: {max_size}")
demo.queue(
    concurrency_count=concurrency_count,
    max_size=max_size,
    default_enabled=True
)
