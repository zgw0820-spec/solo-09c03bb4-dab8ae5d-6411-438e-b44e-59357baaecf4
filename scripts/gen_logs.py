#!/usr/bin/env python3
"""Generate deterministic proxy-log CSVs plus the expected report.

CSV columns: request_id,timestamp,status. Timestamps of one request are
distinct (strictly increasing). A request's retries are spread across the
files, like real logs from several proxies. ~10% of requests use ISO-8601
timestamps to prove both timestamp formats work.
"""
from __future__ import annotations

import csv
import json
import os
import random
import sys
from datetime import datetime, timezone

CATEGORIES = ("ok_single", "recovered", "still_5xx", "single_5xx", "ends_4xx")


def generate(outdir: str, n_files: int = 4, n_requests: int = 6000,
             seed: int = 20261120):
    rng = random.Random(seed)
    os.makedirs(outdir, exist_ok=True)
    handles = [open(os.path.join(outdir, f"proxy-{i}.csv"), "w", newline="")
               for i in range(n_files)]
    writers = [csv.writer(h) for h in handles]
    for w in writers:
        w.writerow(["request_id", "timestamp", "status"])
    expected = []
    total_records = 0
    base = 1_760_000_000.0
    for i in range(n_requests):
        rid = f"req-{i:06d}"
        x = rng.random()
        if x < 0.35:                      # single 2xx, no retry
            seq = [200]
        elif x < 0.60:                    # 5xx then recovered
            seq = [rng.choice([500, 502, 503])
                   for _ in range(rng.randint(1, 3))] + [200]
        elif x < 0.80:                    # retried, still 5xx -> IN REPORT
            seq = [rng.choice([500, 502, 503, 504])
                   for _ in range(rng.randint(2, 5))]
        elif x < 0.90:                    # single 5xx, never retried
            seq = [rng.choice([500, 503])]
        else:                             # retried, ends 4xx
            seq = [rng.choice([500, 503])
                   for _ in range(rng.randint(1, 2))] + [404]
        use_iso = rng.random() < 0.10
        ts = base + rng.uniform(0, 5000)
        last = None
        for st in seq:
            ts += rng.uniform(0.5, 5)     # strictly increasing -> distinct
            if use_iso:
                last = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            else:
                last = f"{ts:.6f}"
            writers[rng.randrange(n_files)].writerow([rid, last, st])
            total_records += 1
        if len(seq) >= 2 and 500 <= seq[-1] < 600:
            expected.append({"request_id": rid, "attempts": len(seq),
                             "latest_status": seq[-1], "latest_ts": last})
    # a few malformed rows per file, to prove they are skipped
    for w in writers:
        w.writerow(["", "not-a-timestamp", "abc"])
        w.writerow(["broken-row"])
    for h in handles:
        h.close()
    expected.sort(key=lambda r: r["request_id"])
    with open(os.path.join(outdir, "expected.json"), "w") as f:
        json.dump(expected, f, indent=1)
    stats = {"total_records": total_records, "total_requests": n_requests}
    return expected, stats


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/demo-logs"
    exp, stats = generate(out)
    print(f"wrote logs to {out}: {stats['total_records']} records, "
          f"{stats['total_requests']} requests, {len(exp)} expected in report")
