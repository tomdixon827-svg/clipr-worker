import os, json, subprocess, base64
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

def upload_file(job_id, file_path):
    import urllib.request
    api_url = "https://clipr-api-production-a3cf.up.railway.app/api/internal/store/" + job_id
    with open(file_path, 'rb') as f:
        data = f.read()
    boundary = 'boundary123456'
    body = ('--' + boundary + '\r\n' +
            'Content-Disposition: form-data; name="file"; filename="output.mp4"\r\n' +
            'Content-Type: video/mp4\r\n\r\n').encode() + data + ('\r\n--' + boundary + '--\r\n').encode()
    req = urllib.request.Request(api_url, data=body, method='POST')
    req.add_header('Content-Type', 'multipart/form-data; boundary=' + boundary)
    urllib.request.urlopen(req)
    return "https://clipr-api-production-a3cf.up.railway.app/api/clips/" + job_id + "/download"

@celery_app.task(name="tasks.process_upload")
def process_upload(job_id, file_b64):
    print("STARTING JOB", job_id, flush=True)
    try:
        publish(job_id, {"status": "processing", "progress": 30})
        out_dir = "/tmp/" + job_id
        os.makedirs(out_dir, exist_ok=True)
        video_path = out_dir + "/source.mp4"
        with open(video_path, "wb") as f:
            f.write(base64.b64decode(file_b64))
        print("FILE WRITTEN", flush=True)
        publish(job_id, {"status": "processing", "progress": 50})
        output_path = process_video(job_id, video_path)
        print("FFMPEG DONE", flush=True)
        publish(job_id, {"status": "uploading", "progress": 85})
        download_url = upload_file(job_id, output_path)
        print("UPLOADED:", download_url, flush=True)
        publish(job_id, {"status": "complete", "progress": 100, "download_url": download_url})
    except Exception as e:
        print("ERROR:", str(e), flush=True)
        publish(job_id, {"status": "failed", "error": str(e), "progress": 0})

def process_video(job_id, video_path):
    out_dir = "/tmp/" + job_id
    output_path = out_dir + "/output.mp4"
    vf = "crop=ih*9/16:ih,scale=1080:1920"
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-vf", vf,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        output_path,
    ]
    run_cmd(cmd)
    return output_path
