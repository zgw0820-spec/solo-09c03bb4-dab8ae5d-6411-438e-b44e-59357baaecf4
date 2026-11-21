#!/usr/bin/env python3
"""端到端核对：同一批日志，正常归并 vs 归并途中停掉一个进程。

Checks:
  A. normal run's report == expected report (merge correctness)
  B. report is locked (HTTP 409) until every merge commits
  C. kill a worker mid-merge -> its shards are lost, recomputed by the
     survivor; intact shards of the survivor are reused (not recomputed)
  D. merges that read a lost shard are invalidated and re-run; the late
     commit of the invalidated task is rejected and never enters the report
  E. the post-kill report is byte-identical to the normal report

Stdlib only. Usage: python3 scripts/verify.py [base-url]
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

BASE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gen_logs import generate  # noqa: E402

CHECKS = []


def check(name: str, ok: bool, extra: str = "") -> None:
    CHECKS.append((name, ok, extra))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({extra})" if extra else ""))


def http(method, path, body=None, files=None, expect=None):
    url = BASE + path
    data = None
    headers = {}
    if files is not None:
        boundary = uuid.uuid4().hex
        parts = []
        for field, filename, content in files:
            parts.append(
                f"--{boundary}\r\nContent-Disposition: form-data; "
                f"name=\"{field}\"; filename=\"{filename}\"\r\n"
                f"Content-Type: text/csv\r\n\r\n".encode())
            parts.append(content)
            parts.append(b"\r\n")
        parts.append(f"--{boundary}--\r\n".encode())
        data = b"".join(parts)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            code, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, payload = e.code, e.read()
    if expect is not None and code != expect:
        raise AssertionError(
            f"{method} {path}: got {code}, expected {expect}: {payload[:300]!r}")
    return code, (json.loads(payload) if payload else {})


def wait_done(jid, timeout=240, on_status=None):
    t0 = time.time()
    while time.time() - t0 < timeout:
        _, st = http("GET", f"/api/jobs/{jid}/status", expect=200)
        if on_status:
            on_status(st)
        if st["done"]:
            return st
        time.sleep(0.1)
    raise TimeoutError(f"job {jid} not done within {timeout}s")


def pick_victim(st, maps):
    """A live worker that holds at least one file's shards; prefer worker1."""
    holders = {}
    for fid, w in maps.items():
        if w:
            holders.setdefault(w, set()).add(fid)
    for cand in sorted(holders):
        if st["workers"].get(cand, {}).get("state") == "ALIVE":
            return cand, holders[cand]
    return None, set()


def main() -> int:
    print(f"== 0. 等待服务 {BASE}")
    for _ in range(60):
        try:
            http("GET", "/health", expect=200)
            break
        except Exception:
            time.sleep(1)

    print("== 1. 生成测试日志")
    tmp = tempfile.mkdtemp(prefix="logs-")
    expected, stats = generate(tmp, n_files=4, n_requests=6000)
    print(f"   {stats['total_records']} 条记录 / {stats['total_requests']} 个请求, "
          f"期望清单 {len(expected)} 个请求")

    print("== 2. 上传并启动任务 A（正常归并）")
    files = [("files", fn, open(os.path.join(tmp, fn), "rb").read())
             for fn in sorted(os.listdir(tmp)) if fn.endswith(".csv")]
    _, up = http("POST", "/api/upload", files=files, expect=200)
    fids = up["file_ids"]
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    job_a = r["job_id"]
    st_a = wait_done(job_a)
    rep_a = st_a["report"]
    check("A 正常完成", True, f"job={job_a}")
    check("A 清单 == 期望清单", rep_a["still_5xx"] == expected,
          f"{len(rep_a['still_5xx'])} vs {len(expected)}")
    check("A 记录总数正确", rep_a["total_records"] == stats["total_records"],
          f"{rep_a['total_records']} vs {stats['total_records']}")
    check("A 请求总数正确", rep_a["total_requests"] == stats["total_requests"],
          f"{rep_a['total_requests']} vs {stats['total_requests']}")

    print("== 3. 任务 B：归并途中停掉一个进程")
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    job_b = r["job_id"]
    code, _ = http("GET", f"/api/jobs/{job_b}/report")
    check("归并完成前 /report 返回 409（清单不提前显示）", code == 409)

    kill = {"done": False, "victim": None, "maps": {}}

    def watch(st):
        for fid, m in st["maps"].items():
            if m["worker"]:
                kill["maps"][fid] = m["worker"]
        if kill["done"]:
            return
        assigned = sum(1 for rd in st["reduces"].values()
                       if rd["state"] == "ASSIGNED")
        if assigned >= 2:  # both workers mid-merge
            victim, vfiles = pick_victim(st, kill["maps"])
            if victim:
                http("POST", f"/api/jobs/{job_b}/kill/{victim}", expect=200)
                kill.update(done=True, victim=victim, vfiles=vfiles)

    st_b = wait_done(job_b, on_status=watch)
    rep_b = st_b["report"]
    check("归并途中停掉了一个进程", kill["done"],
          f"victim={kill['victim']}")
    if kill["done"]:
        check("被停进程状态为 DEAD",
              st_b["workers"][kill["victim"]]["state"] == "DEAD")
    events = "\n".join(e["msg"] for e in st_b["events"])
    check("事件：分片丢失（LOST）", "LOST" in events)
    check("事件：从原文件补算（recompute）", "recompute scheduled" in events)
    check("事件：补算完成（recomputed after shard loss）",
          "recomputed after shard loss" in events)
    check("事件：读过丢失分片的归并作废重跑（INVALIDATED）",
          "INVALIDATED" in events)
    check("事件：作废任务的迟到结果被拒（rejected late reduce commit）",
          "rejected late reduce commit" in events)
    recomputed = {fid for fid, m in st_b["maps"].items() if m["epoch"] > 1}
    check("只补算掉线进程的分片，幸存进程的完好分片全部复用",
          kill["done"] and recomputed == kill["vfiles"],
          f"recomputed={sorted(recomputed)} victim's={sorted(kill['vfiles'])}")
    check("B 清单 == A 清单（掉线后结果一致）",
          rep_b["still_5xx"] == rep_a["still_5xx"],
          f"{len(rep_b['still_5xx'])} vs {len(rep_a['still_5xx'])}")
    keys = ("total_records", "total_requests", "retried", "single_5xx")
    check("B 汇总 == A 汇总",
          {k: rep_b[k] for k in keys} == {k: rep_a[k] for k in keys})
    check("B 清单 == 期望清单", rep_b["still_5xx"] == expected)

    print("\n== 任务 B 事件回放 ==")
    for e in st_b["events"]:
        print(f"   [{e['ts']}] {e['msg']}")

    failed = [c for c in CHECKS if not c[1]]
    print(f"\n{'全部通过' if not failed else '存在失败'}: "
          f"{len(CHECKS) - len(failed)}/{len(CHECKS)} 项通过")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
