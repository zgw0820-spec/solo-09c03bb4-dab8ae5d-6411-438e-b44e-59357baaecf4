"""Shared helpers: CSV parsing, request-id partitioning, record merging."""
from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone

# Canonical CSV header: request_id,timestamp,status  (aliases accepted)
_HEADER_ALIASES = {
    "request_id": {"request_id", "requestid", "req_id", "request", "id"},
    "timestamp": {"timestamp", "ts", "time", "datetime"},
    "status": {"status", "status_code", "http_status", "code"},
}


def partition_of(request_id: str, nparts: int) -> int:
    """Deterministic partition of a request id (stable across processes)."""
    h = hashlib.md5(request_id.encode("utf-8")).hexdigest()
    return int(h, 16) % nparts


def timestamp_key(raw: str) -> float:
    """Sortable key for a timestamp: epoch number or ISO-8601 string."""
    s = raw.strip()
    try:
        return float(s)
    except ValueError:
        pass
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def parse_csv(path):
    """Yield ("ok", record) or ("bad", None) per data row.

    record = {"rid": str, "ts": raw timestamp str, "k": sortable float, "s": int status}
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return
        cols = {}
        for idx, name in enumerate(header):
            n = name.strip().lower()
            for canon, aliases in _HEADER_ALIASES.items():
                if n in aliases:
                    cols[canon] = idx
        if len(cols) < 3:
            raise ValueError(
                f"{path}: CSV header must contain request_id,timestamp,status "
                f"(got {header!r})"
            )
        for row in reader:
            if not row or all(not c.strip() for c in row):
                continue
            try:
                rid = row[cols["request_id"]].strip()
                ts = row[cols["timestamp"]].strip()
                st = int(row[cols["status"]].strip())
                if not rid:
                    raise ValueError("empty request_id")
                yield "ok", {"rid": rid, "ts": ts, "k": timestamp_key(ts), "s": st}
            except (ValueError, IndexError, KeyError):
                yield "bad", None


def merge_record(rec: dict, merged: dict) -> None:
    """Fold one shard record into merged {rid: entry}; latest timestamp wins.

    Timestamps of one request are distinct per the input contract; the status
    tie-break only makes the merge deterministic if that contract is violated.
    """
    rid = rec["rid"]
    cur = merged.get(rid)
    if cur is None:
        merged[rid] = {"rid": rid, "k": rec["k"], "s": rec["s"], "ts": rec["ts"],
                       "attempts": 1}
    else:
        cur["attempts"] += 1
        if (rec["k"], rec["s"]) > (cur["k"], cur["s"]):
            cur.update(k=rec["k"], s=rec["s"], ts=rec["ts"])
