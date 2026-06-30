import os, json, subprocess, base64
import redis
from celery import Celery

REDIS_URL = os.environ["REDIS_URL"]
celery_app = Celery("clipr", broker=REDIS_URL, backend=REDIS_URL)
r = redis.from_url(REDIS_URL)
API_BASE = "https://clipr-api-production-a3cf.up.railway.app"

def publish(job_id, data):
    r.set(f"job:{job_id}", json.dumps(data), ex=3600)

def publish_analysis(job_id, data):
    r.set(f"analyze:{job_id}", json.dumps(data), ex=3600)

def run_cmd(cmd, timeout=600):
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr[-500:])
    return result

def upload_file(job_id, file_path):
    import urllib.request
    api_url = API_BASE + "/api/internal/store/" + job_id
    with open(file_path, 'rb') as f:
        data = f.read()
    boundary = 'boundary123456'
    body = ('--' + boundary + '\r\n' +
            'Content-Disposition: form-data; name="file"; filename="output.mp4"\r\n' +
            'Content-Type: video/mp4\r\n\r\n').encode() + data + ('\r\n--' + boundary + '--\r\n').encode()
    req = urllib.request.Request(api_url, data=body, method='POST')
    req.add_header('Content-Type', 'multipart/form-data; boundary=' + boundary)
    urllib.request.urlopen(req)
    return API_BASE + "/api/clips/" + job_id + "/download"


# ============== CLIP EXPORT ==============

@celery_app.task(name="tasks.process_upload")
def process_upload(job_id, file_b64, clip_start=0, clip_end=0):
    try:
        publish(job_id, {"status": "processing", "progress": 30})
        out_dir = "/tmp/" + job_id
        os.makedirs(out_dir, exist_ok=True)
        video_path = out_dir + "/source.mp4"
        with open(video_path, "wb") as f:
            f.write(base64.b64decode(file_b64))
        publish(job_id, {"status": "processing", "progress": 50})
        output_path = process_video(job_id, video_path, clip_start, clip_end)
        publish(job_id, {"status": "uploading", "progress": 85})
        download_url = upload_file(job_id, output_path)
        publish(job_id, {"status": "complete", "progress": 100, "download_url": download_url})
    except Exception as e:
        publish(job_id, {"status": "failed", "error": str(e), "progress": 0})

def process_video(job_id, video_path, clip_start=0, clip_end=0):
    out_dir = "/tmp/" + job_id
    output_path = out_dir + "/output.mp4"
    vf = "crop=ih*9/16:ih,scale=1080:1920"
    cmd = ["ffmpeg", "-y"]
    if clip_start:
        cmd += ["-ss", str(clip_start)]
    cmd += ["-i", video_path]
    if clip_end and clip_end > clip_start:
        cmd += ["-t", str(clip_end - clip_start)]
    cmd += [
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


# ============== AUTO-CLIP ANALYSIS ==============

@celery_app.task(name="tasks.analyze_video")
def analyze_video(job_id, file_b64):
    try:
        out_dir = "/tmp/analyze_" + job_id
        os.makedirs(out_dir, exist_ok=True)
        video_path = out_dir + "/source.mp4"
        with open(video_path, "wb") as f:
            f.write(base64.b64decode(file_b64))

        publish_analysis(job_id, {"status": "processing", "message": "Extracting audio..."})

        # Extract audio for transcription
        audio_path = out_dir + "/audio.wav"
        run_cmd(["ffmpeg", "-y", "-i", video_path, "-ar", "16000", "-ac", "1", audio_path])

        publish_analysis(job_id, {"status": "processing", "message": "Transcribing speech..."})

        # Transcribe with Whisper
        import whisper
        model = whisper.load_model("base")
        result = model.transcribe(audio_path, word_timestamps=False, verbose=False)
        segments = result.get("segments", [])

        if not segments:
            publish_analysis(job_id, {"status": "complete", "highlights": []})
            return

        publish_analysis(job_id, {"status": "processing", "message": "Scoring moments..."})

        # Get audio energy levels across the video (find loud/exciting moments)
        energy_map = get_audio_energy(audio_path)

        # Group segments into natural "thought" chunks of 15-45 seconds
        candidates = build_candidates(segments, energy_map)

        # Score and pick top 4
        candidates.sort(key=lambda c: c["score"], reverse=True)
        top = candidates[:4]
        top.sort(key=lambda c: c["start"])

        publish_analysis(job_id, {"status": "complete", "highlights": top})

    except Exception as e:
        publish_analysis(job_id, {"status": "failed", "error": str(e)})


def get_audio_energy(audio_path):
    """Use ffmpeg's volumedetect/astats per-second to build a rough energy map."""
    cmd = [
        "ffmpeg", "-i", audio_path, "-af",
        "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level:file=-",
        "-f", "null", "-"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    energies = []
    for line in result.stderr.split("\n") + result.stdout.split("\n"):
        if "RMS_level" in line and "=" in line:
            try:
                val = float(line.split("=")[-1].strip())
                energies.append(val)
            except ValueError:
                pass
    return energies


def build_candidates(segments, energy_map):
    """Group whisper segments into 15-45s chunks at natural sentence boundaries,
    score by speech density, energy, and presence of strong keywords."""
    HOOK_WORDS = ["never", "secret", "wrong", "mistake", "biggest", "best", "worst",
                  "shocking", "amazing", "stop", "wait", "actually", "truth", "nobody",
                  "everyone", "always", "incredible", "insane", "crazy", "important"]

    candidates = []
    chunk = []
    chunk_start = None

    for seg in segments:
        if chunk_start is None:
            chunk_start = seg["start"]
        chunk.append(seg)
        duration = seg["end"] - chunk_start

        # End chunk on natural sentence break once we hit 15-45s
        text_so_far = " ".join(s["text"] for s in chunk)
        ends_sentence = seg["text"].strip().endswith((".", "!", "?"))

        if duration >= 15 and (ends_sentence or duration >= 45):
            candidates.append(score_chunk(chunk, chunk_start, seg["end"], text_so_far, HOOK_WORDS))
            chunk = []
            chunk_start = None

    if chunk and chunk_start is not None:
        text_so_far = " ".join(s["text"] for s in chunk)
        end = chunk[-1]["end"]
        if end - chunk_start >= 8:
            candidates.append(score_chunk(chunk, chunk_start, end, text_so_far, HOOK_WORDS))

    return candidates


def score_chunk(chunk, start, end, text, hook_words):
    duration = max(1, end - start)
    word_count = len(text.split())
    pace = word_count / duration  # words per second - higher = punchier delivery

    text_lower = text.lower()
    hook_score = sum(1 for w in hook_words if w in text_lower)

    # Ideal clip length is 20-35s - score peaks there
    length_score = 1.0 - abs(duration - 27) / 40

    score = round((pace * 10) + (hook_score * 8) + (length_score * 15), 1)

    return {
        "start": round(start, 1),
        "end": round(end, 1),
        "text": text.strip()[:140],
        "score": max(1, min(99, int(score))),
    }
