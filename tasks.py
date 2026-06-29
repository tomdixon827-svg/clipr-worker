import os, json, subprocess, urllib.request
import redis
from celery import Celery

REDIS_URL = os.environ["REDIS_URL"]
celery_app = Celery("clipr", broker=REDIS_URL, backend=REDIS_URL)
r = redis.from_url(REDIS_URL)

def publish(job_id, data):
    r.set(f"job:{job_id}", json.dumps(data), ex=3600)

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(result.stderr[-500:])

def upload_file(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        data = f.read()
    boundary = 'boundary123456'
    body = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="file"; filename="output.mp4"\r\n'
        f'Content-Type: video/mp4\r\n\r\n'
    ).encode() + data + f'\r\n--{boundary}--\r\n'.encode()
    req = urllib.request.Request(
        'https://file.io/?expires=1d',
        data=body,
        method='POST'
    )
    req.add_header('Content-Type', f'multipart/form-data; boundary={boundary}')
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    return result['link']

@celery_app.task(name="tasks.process_upload")
def process_upload(job_id, video_path):
    try:
        publish(job_id, {"status": "processing", "progress": 50})
        output_path = process_video(job_id,
