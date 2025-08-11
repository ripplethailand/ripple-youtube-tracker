# RIPPLE — YouTube View Tracker (Daily + Milestones)

This kit collects daily snapshots from **YouTube Data API v3** and computes milestones: **24h, 7d, 15d, 30d, 90d**, plus **release-day EOD (Asia/Bangkok)**.

## Quick start
1. Put your video IDs in `video_ids.csv`.
2. Set `YT_API_KEY` in your shell and run:
   ```powershell
   pip install -r requirements.txt
   python fetch_youtube_stats.py
   python compute_milestones.py
   ```
3. Check `data/daily_stats.csv` and `data/milestones.csv`.

## GitHub Actions
- Add `YT_API_KEY` as a repository secret.
- The workflow `.github/workflows/daily.yml` runs **daily at 00:00 Bangkok (17:00 UTC)** and commits results.