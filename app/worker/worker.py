"""Worker process.

Executes map tasks (file -> R shards, stored on its own local disk) and
reduce tasks (merge shards of one partition fetched from peer workers,
latest record per request id). Shards live on worker-local storage (tmpfs in
Docker), so a stopped worker genuinely loses them; the coordinator then has
surviving workers recompute them from the original CSVs.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import threading
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from ..common import merge_record, parse_csv, partition_of

WORKER_ID = os.environ.get("WORKER_ID", "worker1")
PORT = int(os.environ.get("WORKER_PORT", "8101"))
COORD = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
SHARD_DIR = os.environ.get("SHARD_DIR", f"/tmp/shards-{WORKER_ID}")
BASE_URL = os.environ.get("WORKER_BASE_URL", f"http://{WORKER_ID}:{PORT}")
REDUCE_DELAY = float(os.environ.get("REDUCE_DELAY_MS", "0")) / 1000.0
# simulated shard-read latency ("读取受阻"), so the job page can show which
# file's shard is being read and which worker serves it
FETCH_DELAY = float(os.environ.get("FETCH_DELAY_MS", "0")) / 1000.0
BOOT_ID = uuid.uuid4().hex[:12]

app = FastAPI(title=f"worker {WORKER_ID}")
_current = {"task": None}


@app.get("/health")
def health():
    return {"ok": True, "worker": WORKER_ID, "boot": BOOT_ID}


@app.get("/shard-files/{path:path}")
def shard_files(path: str):
    if ".." in path:
        raise HTTPException(400, "bad path")
    full = os.path.join(SHARD_DIR, path)
    if not os.path.isfile(full):
        raise HTTPException(404, "no such shard")
    return FileResponse(full)


@app.post("/control/kill")
def control_kill():
    # answer first, then die - simulates a stopped/crashed process
    threading.Timer(0.3, lambda: os._exit(1)).start()
    return {"ok": True, "msg": f"{WORKER_ID} stopping"}


@app.get("/control/status")
def control_status():
    return {"worker": WORKER_ID, "boot": BOOT_ID, "current_task": _current["task"]}


# ------------------------------------------------------------------- tasks

def report_progress(client: httpx.Client, jid: str, part: int, epoch: int,
                    stage: str, file_id: str | None = None,
                    source: str | None = None) -> None:
    """Best-effort reduce-progress report. The merge must go on even when
    the coordinator cannot be told - progress is for the job page only."""
    try:
        client.post(
            f"{COORD}/api/tasks/reduce-progress/{WORKER_ID}",
            json={"job_id": jid, "partition": part, "epoch": epoch,
                  "boot_id": BOOT_ID, "stage": stage,
                  "file_id": file_id, "source": source},
            timeout=3)
    except httpx.HTTPError:
        pass


def do_map(task: dict, client: httpx.Client) -> None:
    jid, fid, epoch, r = task["job_id"], task["file_id"], task["epoch"], task["r"]
    buckets = [[] for _ in range(r)]
    total = bad = 0
    for status, rec in parse_csv(task["csv_path"]):
        if status == "ok":
            buckets[partition_of(rec["rid"], r)].append(rec)
            total += 1
        else:
            bad += 1
    outdir = os.path.join(SHARD_DIR, jid, fid)
    os.makedirs(outdir, exist_ok=True)
    shards = []
    for p in range(r):
        path = os.path.join(outdir, f"part-{p}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for rec in buckets[p]:
                f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        shards.append({"part": p, "records": len(buckets[p])})
    resp = client.post(
        f"{COORD}/api/tasks/map-commit/{WORKER_ID}",
        json={"job_id": jid, "file_id": fid, "epoch": epoch, "boot_id": BOOT_ID,
              "shards": shards},
        timeout=10)
    if resp.status_code == 409:
        shutil.rmtree(outdir, ignore_errors=True)  # stale output - discard
        print(f"[{WORKER_ID}] stale map commit for {fid} rejected - shards discarded",
              flush=True)
        return
    resp.raise_for_status()
    print(f"[{WORKER_ID}] map {fid} done: {total} records "
          f"({bad} bad rows skipped)", flush=True)


def do_reduce(task: dict, client: httpx.Client) -> None:
    jid, part, epoch = task["job_id"], task["partition"], task["epoch"]
    merged: dict = {}
    for inp in task["inputs"]:
        report_progress(client, jid, part, epoch, "reading",
                        file_id=inp["file_id"], source=inp["worker_id"])
        if FETCH_DELAY:
            time.sleep(FETCH_DELAY)  # simulate a slow/blocked shard read
        text = None
        if inp["worker_id"] == WORKER_ID:
            try:
                with open(os.path.join(SHARD_DIR, jid, inp["file_id"],
                                       f"part-{part}.jsonl"),
                          encoding="utf-8") as f:
                    text = f.read()
            except OSError:
                text = None
        else:
            for _ in range(3):
                try:
                    r = client.get(inp["url"], timeout=5)
                    if r.status_code == 200:
                        text = r.text
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.3)
        if text is None:
            client.post(
                f"{COORD}/api/tasks/fail/{WORKER_ID}",
                json={"type": "reduce", "job_id": jid, "partition": part,
                      "epoch": epoch, "boot_id": BOOT_ID,
                      "reason": f"cannot fetch shard from {inp['worker_id']}",
                      "missing": {"file_id": inp["file_id"], "partition": part,
                                  "peer": inp["worker_id"]}},
                timeout=10)
            return
        for line in text.splitlines():
            if line:
                merge_record(json.loads(line), merged)
    report_progress(client, jid, part, epoch, "merging")
    if REDUCE_DELAY:
        # simulate merge work; also widens the window in which a mid-merge
        # kill invalidates this task, so the fencing path is exercised
        time.sleep(REDUCE_DELAY)
    records = sorted(
        ({"rid": e["rid"], "attempts": e["attempts"], "s": e["s"], "ts": e["ts"]}
         for e in merged.values()),
        key=lambda e: e["rid"])
    os.makedirs(os.path.dirname(task["tmp_out"]), exist_ok=True)
    with open(task["tmp_out"], "w", encoding="utf-8") as f:
        json.dump({"partition": part, "records": records}, f)
    resp = client.post(
        f"{COORD}/api/tasks/reduce-commit/{WORKER_ID}",
        json={"job_id": jid, "partition": part, "epoch": epoch, "boot_id": BOOT_ID,
              "tmp_path": task["tmp_out"], "records": len(records)},
        timeout=10)
    if resp.status_code == 409:
        try:
            os.unlink(task["tmp_out"])
        except OSError:
            pass
        print(f"[{WORKER_ID}] late commit for reduce {part} rejected by "
              f"coordinator - discarded", flush=True)
        return
    resp.raise_for_status()
    print(f"[{WORKER_ID}] reduce {part} committed ({len(records)} requests)",
          flush=True)


def run_task(task: dict, client: httpx.Client) -> None:
    if task["type"] == "map":
        do_map(task, client)
    else:
        do_reduce(task, client)


# ------------------------------------------------------------------- loops

async def register(client: httpx.AsyncClient) -> None:
    while True:
        try:
            r = await client.post(
                f"{COORD}/api/workers/register",
                json={"worker_id": WORKER_ID, "boot_id": BOOT_ID,
                      "base_url": BASE_URL},
                timeout=5)
            if r.status_code == 200:
                print(f"[{WORKER_ID}] registered at {COORD} (boot {BOOT_ID})",
                      flush=True)
                return
        except httpx.HTTPError as e:
            print(f"[{WORKER_ID}] waiting for coordinator: {e!r}", flush=True)
        await asyncio.sleep(1.5)


async def heartbeat_loop(client: httpx.AsyncClient) -> None:
    while True:
        await asyncio.sleep(2)
        try:
            await client.post(f"{COORD}/api/workers/{WORKER_ID}/heartbeat",
                              json={"boot_id": BOOT_ID, "base_url": BASE_URL},
                              timeout=3)
        except httpx.HTTPError:
            pass


async def work_loop(client: httpx.AsyncClient) -> None:
    sync = httpx.Client(timeout=10)
    while True:
        try:
            r = await client.get(f"{COORD}/api/tasks/next",
                                 params={"worker_id": WORKER_ID,
                                         "boot_id": BOOT_ID},
                                 timeout=5)
            if r.status_code == 403:
                await register(client)
                continue
            task = r.json().get("task")
            if not task:
                await asyncio.sleep(0.4)
                continue
            _current["task"] = {"type": task["type"], "job": task["job_id"]}
            try:
                await asyncio.to_thread(run_task, task, sync)
            except Exception as e:  # noqa: BLE001 - report and keep working
                print(f"[{WORKER_ID}] task failed: {e!r}", flush=True)
                try:
                    sync.post(
                        f"{COORD}/api/tasks/fail/{WORKER_ID}",
                        json={"type": task["type"], "job_id": task["job_id"],
                              "epoch": task["epoch"], "boot_id": BOOT_ID,
                              "reason": repr(e),
                              "partition": task.get("partition"),
                              "file_id": task.get("file_id")},
                        timeout=5)
                except httpx.HTTPError:
                    pass
            finally:
                _current["task"] = None
        except httpx.HTTPError:
            await asyncio.sleep(1)


async def main() -> None:
    os.makedirs(SHARD_DIR, exist_ok=True)
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    async with httpx.AsyncClient() as client:
        await register(client)
        await asyncio.gather(server.serve(), heartbeat_loop(client),
                             work_loop(client))


if __name__ == "__main__":
    asyncio.run(main())
