"""
luna_anpr.py — Send all .jpg face images from a directory to Luna API

- Loads config from .env (no CLI needed).
- Parses structured filenames and sends compact 'user_data'.
- Deletes image files ONLY when the HTTP upload succeeds (2xx).
- Works on Windows & Linux (close file handles, small unlink retry).

Env (.env):
  FR_API_URL        = http://HOST:PORT/6/handlers/<HANDLER_UUID>/events
  FR_API_USER       = username
  FR_API_PASS       = password
  FACES_DIR         = /path/to/faces
  FIELD_NAME        = image
  SEND_AS_RAW       = 0
  MAX_PARALLEL      = 8
  # Optional query params:
  CITY, AREA, DISTRICT, STREET, HOUSE_NUMBER, LONGITUDE, LATITUDE,
  EXTERNAL_ID, SOURCE, STREAM_ID, TAGS, TRACK_ID, USE_EXIF_INFO
"""

import os
import re
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import random
import time

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ── Config ─────────────────────────────────────────────────────
API_URL   = os.getenv("FR_API_URL", "").strip()
API_USER  = os.getenv("FR_API_USER", "").strip()
API_PASS  = os.getenv("FR_API_PASS", "").strip()
FACES_DIR = os.getenv("FACES_DIR", "").strip()

FIELD_NAME   = os.getenv("FIELD_NAME", "image").strip()
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL", "8"))
SEND_AS_RAW  = os.getenv("SEND_AS_RAW", "0") == "1"

# Optional query params (add only if present)
QP_MAP = {
    "city":         os.getenv("CITY"),
    "area":         os.getenv("AREA"),
    "district":     os.getenv("DISTRICT"),
    "street":       os.getenv("STREET"),
    "house_number": os.getenv("HOUSE_NUMBER"),
    "longitude":    os.getenv("LONGITUDE"),
    "latitude":     os.getenv("LATITUDE"),
    "external_id":  os.getenv("EXTERNAL_ID"),
    "source":       os.getenv("SOURCE"),
    "stream_id":    os.getenv("STREAM_ID"),
    "tags":         os.getenv("TAGS"),
    "track_id":     os.getenv("TRACK_ID"),
    "use_exif_info":os.getenv("USE_EXIF_INFO", "1"),  # default 1
}

# ── Dictionaries for token boundaries (scanner) ────────────────
VIOLATION_SET = ["Speeding", "ANPR", "RedLight", "Overspeed", "Speed"]
LICENSE_SET   = ["Unlicensed", "Licensed", "Registered", "Unregistered", "Unknown"]
VEHICLE_CLASSES = [
    "Small-sized Vehicle","Medium-sized Vehicle","Large-sized Vehicle",
    "Small Vehicle","Large Vehicle","Motorcycle","Car","Truck","Bus",
]
CAMERA_MODES = ["Panoramic", "Portrait", "Wide", "Tele"]   # ignored in user_data
TRAJECTORIES = ["Straight", "Left", "Right", "UTurn", "U-Turn"]

def _match_prefix(s: str, options: list[str]) -> Optional[str]:
    for opt in sorted(options, key=len, reverse=True):
        if s.startswith(opt):
            return opt
    return None

def _safe_iso(cap_date: str, cap_time: str) -> str:
    # Event time from first timestamp (field #1)
    dt = datetime.strptime(cap_date + cap_time, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def parse_image_filename(fname: str) -> Dict[str, Any]:
    """
    Robust parser. If anything fails, returns partial meta (never blocks sending).
    """
    meta: Dict[str, Any] = {}
    try:
        # #1 capture date/time
        m = re.match(r"^(\d{8})_(\d{6})_", fname)
        if m:
            meta["cap_date"] = m.group(1)
            meta["cap_time"] = m.group(2)
        # IP
        ipm = re.search(r"(\d{1,3}\.){3}\d{1,3}", fname)
        if ipm:
            meta["ip"] = ipm.group(0)
        # violation + plate
        vm = re.search(r"(Speeding|ANPR|RedLight|Overspeed|Speed)([A-Z0-9]+)", fname)
        if vm:
            meta["violation"] = vm.group(1)
            meta["plate"] = vm.group(2)
        # class
        vcm = re.search(r"(Small-sized Vehicle|Medium-sized Vehicle|Large-sized Vehicle|Small Vehicle|Large Vehicle|Motorcycle|Car|Truck|Bus)", fname)
        if vcm:
            meta["vehicle_class"] = vcm.group(1)
        # primary color (word after 'Vehicle')
        colm = re.search(r"Vehicle([A-Za-z ]+)", fname)
        if colm:
            meta["primary_color"] = colm.group(1).strip()
        # trajectory
        tm = re.search(r"(Straight|Left|Right|UTurn|U-Turn)", fname)
        if tm:
            meta["trajectory"] = tm.group(1)
        # sequence (digits right before .Face_X.jpg)
        seqm = re.search(r"(\d+)\.Face_\d+\.jpg$", fname)
        if seqm:
            meta["sequence"] = int(seqm.group(1))
        # crop index
        cropm = re.search(r"\.Face_(\d+)\.jpg$", fname)
        if cropm:
            meta["crop_index"] = int(cropm.group(1))
    except Exception:
        pass
    if not meta:
        meta["filename"] = fname
    return meta

def build_user_data(meta: Dict[str, Any]) -> str:
    """
    Compact 'user_data' (<=128 chars) with ONLY the required fields:
    1 (date+time), 3 (ip), 6 (violation), 7 (plate), 9 (class),
    10 (primary color), 15 (trajectory), 16 (sequence), 17 (crop index).
    """
    parts = [
        f"dt={meta.get('cap_date','')}-{meta.get('cap_time','')}",
        f"ip={meta.get('ip','')}",
        f"viol={meta.get('violation','')}",
        f"plate={meta.get('plate','')}",
        f"class={meta.get('vehicle_class','')}",
        f"color={meta.get('primary_color','')}",
        f"traj={meta.get('trajectory','')}",
        f"seq={meta.get('sequence','')}",
        f"crop={meta.get('crop_index','')}",
    ]
    s = ";".join(parts)
    return s if len(s) <= 128 else (s[:125] + "...")

def _build_query_params(meta: Dict[str, Any]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for k, v in QP_MAP.items():
        if not v:
            continue
        if k in ("longitude", "latitude"):
            try: params[k] = float(v)
            except ValueError: continue
        elif k in ("use_exif_info",):
            try: params[k] = int(v)
            except ValueError: continue
        else:
            params[k] = v
    params["user_data"] = build_user_data(meta)
    return params

async def _post_with_retries(client: httpx.AsyncClient, *, url, files=None, content=None,
                             headers=None, auth=None, params=None, attempts=4) -> httpx.Response:
    backoff = 0.3
    last_exc = None
    for i in range(attempts):
        try:
            r = await client.post(url, files=files, content=content,
                                  headers=headers, auth=auth, params=params)
            if 200 <= r.status_code < 300:
                return r
            # retry 5xx
            if 500 <= r.status_code < 600 and i < attempts - 1:
                await asyncio.sleep(backoff + random.random() * 0.2)
                backoff *= 2
                continue
            return r
        except Exception as e:
            last_exc = e
            if i < attempts - 1:
                await asyncio.sleep(backoff + random.random() * 0.2)
                backoff *= 2
                continue
            raise last_exc

def _unlink_hard(path: Path, max_wait_ms: int = 500) -> bool:
    """
    Cross-platform unlink with a tiny retry loop (handles transient Windows locks).
    Returns True if deleted, False otherwise.
    """
    deadline = time.time() + max_wait_ms / 1000.0
    while True:
        try:
            path.unlink()
            return True
        except Exception:
            if time.time() >= deadline:
                return False
            time.sleep(0.05)

async def send_all_images():
    if not API_URL or not API_USER or not API_PASS or not FACES_DIR:
        raise SystemExit("ERROR: FR_API_URL, FR_API_USER, FR_API_PASS, and FACES_DIR must be set")

    faces_dir_path = Path(FACES_DIR)
    if not faces_dir_path.exists() or not faces_dir_path.is_dir():
        raise SystemExit(f"Faces folder not found: {faces_dir_path}")

    images = sorted(faces_dir_path.rglob("*.jpg"))  # ONLY IMAGES
    total = len(images)
    if total == 0:
        print(f"No .jpg files found in {faces_dir_path}")
        return

    sent_ok = 0
    parse_ok = 0
    parse_fail = 0
    status_counts: Dict[int, int] = {}

    limits  = httpx.Limits(max_connections=MAX_PARALLEL)
    timeout = httpx.Timeout(30.0, read=30.0, write=30.0, connect=10.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(MAX_PARALLEL)

        async def worker(img: Path):
            nonlocal sent_ok, parse_ok, parse_fail
            async with sem:
                meta = parse_image_filename(img.name)
                if meta:
                    parse_ok += 1
                else:
                    parse_fail += 1

                headers: Dict[str, str] = {}
                if meta.get("cap_date") and meta.get("cap_time"):
                    headers["Luna-Event-Time"] = _safe_iso(meta["cap_date"], meta["cap_time"])

                params = _build_query_params(meta)

                try:
                    if SEND_AS_RAW:
                        with img.open("rb") as f:
                            r = await _post_with_retries(
                                client,
                                url=API_URL,
                                content=f.read(),
                                headers={**headers, "Content-Type": "image/jpeg"},
                                auth=(API_USER, API_PASS),
                                params=params,
                            )
                    else:
                        with img.open("rb") as f:
                            files = {FIELD_NAME: (img.name, f, "image/jpeg")}
                            r = await _post_with_retries(
                                client,
                                url=API_URL,
                                files=files,
                                headers=headers,
                                auth=(API_USER, API_PASS),
                                params=params,
                            )
                except Exception as e:
                    stamp = datetime.now().strftime("%H:%M:%S")
                    print(f"[{stamp}] ✗ {img.name} EXCEPTION: {e}")
                    status_counts[-1] = status_counts.get(-1, 0) + 1
                    return

                status_counts[r.status_code] = status_counts.get(r.status_code, 0) + 1
                stamp = datetime.now().strftime("%H:%M:%S")
                # if 200 <= r.status_code < 300:
                #     print(f"[{stamp}] ✓ {img.name} → {r.status_code}")
                #     sent_ok += 1
                #     # DELETE ONLY ON SUCCESS (Windows/Linux safe)
                #     if not _unlink_hard(img):
                #         print(f"[WARN] Could not delete {img}")
                # else:
                #     preview = r.text[:200].replace("\n", " ")
                #     print(f"[{stamp}] ✗ {img.name} → {r.status_code}: {preview}")
                if 200 <= r.status_code < 300:
                    print(f"[{stamp}] ✓ {img.name} → {r.status_code}")
                    sent_ok += 1
                    # delete good sends
                    if not _unlink_hard(img):
                        print(f"[WARN] Could not delete {img}")

                elif 400 <= r.status_code < 500:
                    # client error → no point retrying, delete permanently
                    preview = r.text[:200].replace("\n", " ")
                    print(f"[{stamp}] ✗ {img.name} → {r.status_code} (client error, deleted): {preview}")
                    if not _unlink_hard(img):
                        print(f"[WARN] Could not delete {img}")

                else:
                    # keep on 5xx or other errors for retry
                    preview = r.text[:200].replace("\n", " ")
                    print(f"[{stamp}] ✗ {img.name} → {r.status_code} (kept): {preview}")


        await asyncio.gather(*(worker(img) for img in images))

    print(f"Total images: {total}, Sent OK: {sent_ok}, Parse OK: {parse_ok}, Parse Fail: {parse_fail}")
    # Print status code histogram to diagnose “520 at end”
    if status_counts:
        print("HTTP status counts:")
        for code in sorted(status_counts.keys()):
            label = "EXC" if code == -1 else str(code)
            print(f"  {label}: {status_counts[code]}")

def main():
    asyncio.run(send_all_images())

if __name__ == "__main__":
    main()
