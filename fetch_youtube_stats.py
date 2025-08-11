import csv
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict
import requests

API_KEY = os.getenv("YT_API_KEY")
if not API_KEY:
    print("ERROR: Missing YT_API_KEY environment variable.", file=sys.stderr)
    sys.exit(1)

VIDEO_IDS_PATH = "video_ids.csv"
OUTPUT_PATH = "data/daily_stats.csv"
YOUTUBE_VIDEOS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"
BKK_TZ = ZoneInfo("Asia/Bangkok")

def read_video_ids(path: str) -> List[Dict[str, str]]:
    items = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid = (row.get("video_id") or "").strip()
            label = (row.get("label") or "").strip()
            if vid:
                items.append({"video_id": vid, "label": label})
    return items

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def fetch_stats(video_ids: List[str]) -> Dict[str, Dict]:
    results = {}
    for batch in chunked(video_ids, 50):  # YouTube API: up to 50 ids per call
        params = {"part": "snippet,statistics,contentDetails", "id": ",".join(batch), "key": API_KEY}
        r = requests.get(YOUTUBE_VIDEOS_ENDPOINT, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for item in data.get("items", []):
            vid = item["id"]
            snip = item.get("snippet", {})
            stats = item.get("statistics", {})
            results[vid] = {
                "video_id": vid,
                "channel_id": snip.get("channelId", ""),
                "title": snip.get("title", ""),
                "published_at": snip.get("publishedAt", ""),
                "view_count": stats.get("viewCount", ""),
                "like_count": stats.get("likeCount", ""),
                "comment_count": stats.get("commentCount", ""),
            }
    return results

def ensure_output(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "run_date_bkk","run_datetime_utc",
                "video_id","channel_id","title","published_at",
                "view_count","like_count","comment_count","label"
            ])

def main():
    ensure_output(OUTPUT_PATH)
    items = read_video_ids(VIDEO_IDS_PATH)
    if not items:
        print("No video IDs found in video_ids.csv")
        return

    ids = [x["video_id"] for x in items]
    stats = fetch_stats(ids)

    run_dt_utc = datetime.now(timezone.utc)
    run_date_bkk = run_dt_utc.astimezone(BKK_TZ).date().isoformat()
    run_dt_utc_iso = run_dt_utc.isoformat()

    with open(OUTPUT_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for it in items:
            vid = it["video_id"]; label = it["label"]
            s = stats.get(vid)
            if not s:
                writer.writerow([run_date_bkk, run_dt_utc_iso, vid, "", "", "", "", "", "", label])
                continue
            writer.writerow([
                run_date_bkk, run_dt_utc_iso,
                s["video_id"], s["channel_id"], s["title"], s["published_at"],
                s["view_count"], s["like_count"], s["comment_count"], label
            ])

    print(f"Appended {len(items)} rows to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()