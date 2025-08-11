import csv
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo

INPUT = "data/daily_stats.csv"
OUTPUT = "data/milestones.csv"
BKK = ZoneInfo("Asia/Bangkok")

def parse_iso(s: str):
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def to_int(s: str):
    try:
        return int(s)
    except Exception:
        return None

with open(INPUT, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

by_vid = {}
for r in rows:
    vid = r.get("video_id","").strip()
    if not vid:
        continue
    d = by_vid.setdefault(vid, {"label": r.get("label",""), "published_at": None, "snaps": []})
    if r.get("label"):
        d["label"] = r["label"]
    pub = parse_iso(r.get("published_at",""))
    if pub and (d["published_at"] is None or pub < d["published_at"]):
        d["published_at"] = pub
    run_dt = parse_iso(r.get("run_datetime_utc",""))
    vc = to_int(r.get("view_count",""))
    if run_dt and vc is not None:
        d["snaps"].append((run_dt, vc))

for d in by_vid.values():
    d["snaps"].sort(key=lambda x: x[0])

def first_on_or_after(snaps, target_dt_utc):
    for dt, v in snaps:
        if dt >= target_dt_utc:
            return v, dt
    return None, None

fields = [
    "video_id","label","published_at_utc","published_date_bkk",
    "views_24h","snapshot_24h_utc",
    "views_7d","snapshot_7d_utc",
    "views_15d","snapshot_15d_utc",
    "views_30d","snapshot_30d_utc",
    "views_90d","snapshot_90d_utc",
    "views_release_day_eod_bkk","snapshot_eod_bkk_utc"
]

with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(fields)
    for vid, d in by_vid.items():
        pub = d["published_at"]
        if not pub or not d["snaps"]:
            w.writerow([vid, d["label"], "", "", "", "", "", "", "", "", "", "", "", ""])
            continue

        pub_utc = pub.astimezone(timezone.utc)
        pub_date_bkk = pub.astimezone(BKK).date().isoformat()

        targets = {
            "24h": pub_utc + timedelta(days=1),
            "7d":  pub_utc + timedelta(days=7),
            "15d": pub_utc + timedelta(days=15),
            "30d": pub_utc + timedelta(days=30),
            "90d": pub_utc + timedelta(days=90),
        }

        pub_bkk = pub.astimezone(BKK)
        eod_bkk = datetime.combine(pub_bkk.date() + timedelta(days=1), time(0, 0), tzinfo=BKK)
        eod_bkk_utc = eod_bkk.astimezone(timezone.utc)

        v24,  t24  = first_on_or_after(d["snaps"], targets["24h"])
        v7,   t7   = first_on_or_after(d["snaps"], targets["7d"])
        v15,  t15  = first_on_or_after(d["snaps"], targets["15d"])
        v30,  t30  = first_on_or_after(d["snaps"], targets["30d"])
        v90,  t90  = first_on_or_after(d["snaps"], targets["90d"])
        vEOD, tEOD = first_on_or_after(d["snaps"], eod_bkk_utc)

        w.writerow([
            vid, d["label"], pub_utc.isoformat(), pub_date_bkk,
            v24 if v24 is not None else "",  t24.isoformat()  if t24  else "",
            v7  if v7  is not None else "",  t7.isoformat()   if t7   else "",
            v15 if v15 is not None else "",  t15.isoformat()  if t15  else "",
            v30 if v30 is not None else "",  t30.isoformat()  if t30  else "",
            v90 if v90 is not None else "",  t90.isoformat()  if t90 else "",
            vEOD if vEOD is not None else "", tEOD.isoformat() if tEOD else "",
        ])

print(f"Wrote {OUTPUT}")