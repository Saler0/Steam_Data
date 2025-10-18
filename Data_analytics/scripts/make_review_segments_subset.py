#!/usr/bin/env python
from __future__ import annotations

"""Build a review_segments subset config from env vars.

Reads APPIDS (space‑separated), optional SEG_FROM/SEG_TO (YYYY‑MM),
loads configs/review_segments.yaml and writes configs/review_segments_subset.yaml
with a Mongo query filtered by appids and optional timestamp range, plus a
minimal projection to speed up scans.
"""
import os
from pathlib import Path
import datetime as dt
import yaml


def ym_to_ts(s: str | None) -> int | None:
    if not s:
        return None
    try:
        y, m = map(int, s.split("-"))
        return int(dt.datetime(y, m, 1).timestamp())
    except Exception:
        return None


def main() -> None:
    base = Path('configs/review_segments.yaml')
    if not base.exists():
        raise SystemExit(f"Base config not found: {base}")

    cfg = yaml.safe_load(base.read_text(encoding='utf-8')) or {}
    cfg.setdefault('mongo', {})

    apps = [a for a in (os.getenv('APPIDS') or '').split() if a]
    if not apps:
        raise SystemExit('APPIDS env var is empty; nothing to subset')

    q: dict = {'appid': {'$in': apps}}
    ge = ym_to_ts(os.getenv('SEG_FROM'))
    lt = ym_to_ts(os.getenv('SEG_TO'))
    if ge is not None or lt is not None:
        qr: dict = {}
        if ge is not None:
            qr['$gte'] = ge
        if lt is not None:
            qr['$lt'] = lt
        q['timestamp_created'] = qr

    cfg['mongo']['query'] = q
    cfg['mongo']['projection'] = {
        'appid': 1,
        'recommendationid': 1,
        'review_clean': 1,
        'timestamp_created': 1,
        'voted_up': 1,
        'author.playtime_at_review': 1,
        'author.playtime_forever': 1,
        'author.playtime_last_two_weeks': 1,
        'votes_up': 1,
        'votes_funny': 1,
        'comment_count': 1,
        'weighted_vote_score': 1,
    }

    out = Path('configs/review_segments_subset.yaml')
    out.write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True), encoding='utf-8')
    print(f"[OK] Wrote {out}")


if __name__ == '__main__':
    main()

