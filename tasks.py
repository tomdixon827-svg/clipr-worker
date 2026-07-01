import os, json, subprocess, base64
import redis
from celery import Celery

REDIS_URL = os.environ["REDIS_URL"]
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

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

def transcribe_audio(audio_path):
    """OpenAI Whisper API with word-level timestamps."""
    import urllib.request

    with open(audio_path, 'rb') as f:
        audio_data = f.read()

    boundary = 'whisper_boundary_abc123'
    body = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="file"; filename="audio.wav"\r\n'
        f'Content-Type: audio/wav\r\n\r\n'
    ).encode() + audio_data + (
        f'\r\n--{boundary}\r\n'
        f'Content-Disposition: form-data; name="model"\r\n\r\nwhisper-1\r\n'
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="language"\r\n\r\nen\r\n'
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="response_format"\r\n\r\nverbose_json\r\n'
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="timestamp_granularities[]"\r\n\r\nword\r\n'
        f'--{boundary}--\r\n'
    ).encode()

    req = urllib.request.Request(
        'https://api.openai.com/v1/audio/transcriptions',
        data=body, method='POST'
    )
    req.add_header('Authorization', f'Bearer {OPENAI_API_KEY}')
    req.add_header('Content-Type', f'multipart/form-data; boundary={boundary}')

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())

    # Return word-level entries with start/end times
    words = result.get('words', [])
    segments = result.get('segments', [])
    return words, segments

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
def process_upload(job_id, file_b64, clip_start=0, clip_end=0, captions=False):
    print("EXPORT START", job_id, "captions=", captions, flush=True)
    try:
        publish(job_id, {"status": "processing", "progress": 30})
        out_dir = "/tmp/" + job_id
        os.makedirs(out_dir, exist_ok=True)
        video_path = out_dir + "/source.mp4"
        with open(video_path, "wb") as f:
            f.write(base64.b64decode(file_b64))
        print("FILE WRITTEN", flush=True)

        publish(job_id, {"status": "processing", "progress": 45})
        trimmed_path = trim_clip(job_id, video_path, clip_start, clip_end)
        print("TRIMMED", flush=True)

        words = []
        srt_path = None
        if captions:
            publish(job_id, {"status": "processing", "progress": 60, "message": "Generating captions..."})
            print("CAPTIONS: calling OpenAI Whisper API", flush=True)
            audio_path = out_dir + "/clip_audio.wav"
            run_cmd(["ffmpeg", "-y", "-i", trimmed_path, "-ar", "16000", "-ac", "1", audio_path])
            words, segments = transcribe_audio(audio_path)
            srt_path = build_word_srt(out_dir, words)
            print("CAPTIONS DONE, words:", len(words), flush=True)
            # Store word timestamps in Redis so the frontend can display/edit them
            publish(job_id, {
                "status": "processing", "progress": 70,
                "words": words,
                "message": "Rendering..."
            })

        publish(job_id, {"status": "processing", "progress": 75})
        output_path = render_final(job_id, trimmed_path, srt_path)
        print("RENDER DONE", flush=True)

        publish(job_id, {"status": "uploading", "progress": 90})
        download_url = upload_file(job_id, output_path)
        print("UPLOADED:", download_url, flush=True)
        publish(job_id, {
            "status": "complete", "progress": 100,
            "download_url": download_url,
            "words": words
        })
    except Exception as e:
        print("EXPORT ERROR:", str(e), flush=True)
        publish(job_id, {"status": "failed", "error": str(e), "progress": 0})


def trim_clip(job_id, video_path, clip_start, clip_end):
    out_dir = "/tmp/" + job_id
    trimmed_path = out_dir + "/trimmed.mp4"
    cmd = ["ffmpeg", "-y"]
    if clip_start:
        cmd += ["-ss", str(clip_start)]
    cmd += ["-i", video_path]
    if clip_end and clip_end > clip_start:
        cmd += ["-t", str(clip_end - clip_start)]
    cmd += ["-c", "copy", trimmed_path]
    try:
        run_cmd(cmd)
    except RuntimeError:
        cmd = ["ffmpeg", "-y"]
        if clip_start:
            cmd += ["-ss", str(clip_start)]
        cmd += ["-i", video_path]
        if clip_end and clip_end > clip_start:
            cmd += ["-t", str(clip_end - clip_start)]
        cmd += ["-c:v", "libx264", "-preset", "fast", "-c:a", "aac", trimmed_path]
        run_cmd(cmd)
    return trimmed_path


def build_word_srt(out_dir, words):
    """Build an SRT with one word per entry for word-by-word caption display."""
    if not words:
        return None
    srt_path = out_dir + "/captions.srt"
    with open(srt_path, "w") as f:
        for i, w in enumerate(words, start=1):
            start = format_srt_time(w.get("start", 0))
            end = format_srt_time(w.get("end", w.get("start", 0) + 0.4))
            text = w.get("word", "").strip()
            if text:
                f.write(f"{i}\n{start} --> {end}\n{text}\n\n")
    return srt_path


def format_srt_time(seconds):
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds - int(seconds)) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def render_final(job_id, trimmed_path, srt_path):
    out_dir = "/tmp/" + job_id
    output_path = out_dir + "/output.mp4"
    vf_parts = ["crop=ih*9/16:ih", "scale=1080:1920"]
    if srt_path:
        # Bold white text, black outline, positioned 2/3 down (MarginV from bottom)
        style = "FontName=Arial,FontSize=28,Bold=1,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=3,Shadow=1,Alignment=2,MarginV=600"
        escaped_path = srt_path.replace(":", "\\:")
        vf_parts.append(f"subtitles={escaped_path}:force_style='{style}'")
    vf = ",".join(vf_parts)
    cmd = [
        "ffmpeg", "-y",
        "-i", trimmed_path,
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
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
        audio_path = out_dir + "/audio.wav"
        run_cmd(["ffmpeg", "-y", "-i", video_path, "-ar", "16000", "-ac", "1", "-t", "600", audio_path])

        publish_analysis(job_id, {"status": "processing", "message": "Transcribing speech..."})
        words, segments = transcribe_audio(audio_path)

        if not segments:
            publish_analysis(job_id, {"status": "complete", "highlights": []})
            return

        publish_analysis(job_id, {"status": "processing", "message": "Scoring moments..."})
        candidates = build_candidates(segments)
        candidates.sort(key=lambda c: c["score"], reverse=True)
        top = candidates[:4]
        top.sort(key=lambda c: c["start"])

        publish_analysis(job_id, {"status": "complete", "highlights": top})

    except Exception as e:
        publish_analysis(job_id, {"status": "failed", "error": str(e)})


def build_candidates(segments):
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
        text_so_far = " ".join(s["text"] for s in chunk)
        ends_sentence = seg["text"].strip().endswith((".", "!", "?"))

        if duration >= 15 and (ends_sentence or duration >= 45):
            candidates.append(score_chunk(chunk_start, seg["end"], text_so_far, HOOK_WORDS))
            chunk = []
            chunk_start = None

    if chunk and chunk_start is not None:
        text_so_far = " ".join(s["text"] for s in chunk)
        end = chunk[-1]["end"]
        if end - chunk_start >= 5:
            candidates.append(score_chunk(chunk_start, end, text_so_far, HOOK_WORDS))

    return candidates


def score_chunk(start, end, text, hook_words):
    duration = max(1, end - start)
    word_count = len(text.split())
    pace = word_count / duration
    text_lower = text.lower()
    hook_score = sum(1 for w in hook_words if w in text_lower)
    length_score = 1.0 - abs(duration - 27) / 40
    score = round((pace * 10) + (hook_score * 8) + (length_score * 15), 1)
    return {
        "start": round(start, 1),
        "end": round(end, 1),
        "text": text.strip()[:140],
        "score": max(1, min(99, int(score))),
    }
