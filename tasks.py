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
    req = urllib.request.Request(
        'https://store1.gofile.io/uploadFile',
        data=data,
        method='POST'
    )
    req.add_header('Content-Type', 'application/octet-stream')
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    return result['data']['downloadPage']

@celery_app.task(name="tasks.process_youtube")
def process_youtube(job_id, payload):
    try:
        out_dir = f"/tmp/{job_id}"
        os.makedirs(out_dir, exist_ok=True)
        publish(job_id, {"status": "downloading", "progress": 15})
        import yt_dlp
        opts = {
            "format": "bestvideo+bestaudio/best",
            "outtmpl": f"{out_dir}/source.%(ext)s",
            "merge_output_format": "mp4",
            "noplaylist": True,
            "quiet": True,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(payload["url"], download=True)
            video_path = f"{out_dir}/source.mp4"
        publish(job_id, {"status": "processing", "progress": 50})
        output_path = process_video(job_id, video_path, payload)
        publish(job_id, {"status": "uploading", "progress": 85})
        download_url = upload_file(output_path)
        publish(job_id, {"status": "complete", "progress": 100, "download_url": download_url})
    except Exception as e:
        publish(job_id, {"status": "failed", "error": str(e), "progress": 0})

@celery_app.task(name="tasks.process_upload")
def process_upload(job_id, video_path):
    try:
        publish(job_id, {"status": "processing", "progress": 50})
        output_path = process_video(job_id, video_path, {})
        publish(job_id, {"status": "uploading", "progress": 85})
        download_url = upload_file(output_path)
        publish(job_id, {"status": "complete", "progress": 100, "download_url": download_url})
    except Exception as e:
        publish(job_id, {"status": "failed", "error": str(e), "progress": 0})

def process_video(job_id, video_path, payload):
    out_dir = f"/tmp/{job_id}"
    os.makedirs(out_dir, exist_ok=True)
    output_path = f"{out_dir}/output.mp4"
    start = payload.get("clip_start", 0)
    end = payload.get("clip_end", 30)
    duration = max(1, end - start)
    vf = "crop=ih*9/16:ih,scale=1080:1920"
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),
        "-i", video_path,
        "-t", str(duration),
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
