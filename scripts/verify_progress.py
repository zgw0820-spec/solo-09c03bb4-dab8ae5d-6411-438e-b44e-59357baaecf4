#!/usr/bin/env python3
"""端到端核对：任务页分区读取进度（排障可见性）。

覆盖：
  A. 读取受阻时能看到"正在读哪份 CSV 的分片、由哪台进程提供"（reading），
     读齐后显示"正在归并"（merging），分片未齐时列出还在等哪些文件
  B. 重派后迟到的旧进度不能覆盖新一轮进度（epoch/boot/执行者 fencing），
     已提交的分区始终显示完成
  C. 拿不到进度时提示"暂时无法确认"，归并仍照常产出报告（幽灵 worker）
  D. 读取失败时保留最后一次失败的文件和来源，直到分区重试（幽灵 map）

前提：服务栈已启动，且 worker 配置了 FETCH_DELAY_MS>0（模拟读取受阻）、
REDUCE_DELAY_MS>0。Stdlib only。
用法: python3 scripts/verify_progress.py [base-url]
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


def page(jid) -> str:
    with urllib.request.urlopen(f"{BASE}/jobs/{jid}", timeout=10) as resp:
        return resp.read().decode()


def status(jid) -> dict:
    _, st = http("GET", f"/api/jobs/{jid}/status", expect=200)
    return st


def wait_done(jid, timeout=180, on_status=None) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = status(jid)
        if on_status:
            on_status(st)
        if st["done"]:
            return st
        time.sleep(0.05)
    raise TimeoutError(f"job {jid} not done within {timeout}s")


def poll_until(fn, timeout=30, interval=0.05):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return v
        time.sleep(interval)
    return None


def next_task(wid, boot):
    _, r = http("GET", f"/api/tasks/next?worker_id={wid}&boot_id={boot}",
                expect=200)
    return r.get("task")


def post_progress(wid, jid, part, epoch, boot_id, file_id="STALE_SENTINEL"):
    _, r = http("POST", f"/api/tasks/reduce-progress/{wid}",
                body={"job_id": jid, "partition": part, "epoch": epoch,
                      "boot_id": boot_id, "stage": "reading",
                      "file_id": file_id, "source": "STALE_SRC"}, expect=200)
    return r.get("accepted")


def main() -> int:
    print(f"== 0. 等待服务 {BASE}")
    for _ in range(60):
        try:
            http("GET", "/health", expect=200)
            break
        except Exception:
            time.sleep(1)

    print("== 1. 生成测试日志并上传")
    tmp = tempfile.mkdtemp(prefix="logs-prog-")
    expected, stats = generate(tmp, n_files=4, n_requests=4000)
    files = [("files", fn, open(os.path.join(tmp, fn), "rb").read())
             for fn in sorted(os.listdir(tmp)) if fn.endswith(".csv")]
    _, up = http("POST", "/api/upload", files=files, expect=200)
    fids = up["file_ids"]
    print(f"   {stats['total_records']} 条记录, 期望清单 {len(expected)} 个请求")

    # ---------------------------------------------------------- A. 进度可见
    print("== 2. 任务 P1：读取受阻时的进度可见性（reading/merging/waiting）")
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    p1 = r["job_id"]
    seen = {"reading": None, "merging": False, "waiting": None,
            "html_reading": False, "html_merging": False, "html_waiting": False}

    def watch1(st):
        for p, rd in st["reduces"].items():
            pr = rd.get("progress")
            if pr and pr.get("stage") == "reading" and not seen["reading"]:
                seen["reading"] = (pr.get("file_id"), pr.get("source"))
                html = page(p1)
                seen["html_reading"] = "正在读" in html and "提供" in html
            if pr and pr.get("stage") == "merging" and not seen["merging"]:
                seen["merging"] = True
                seen["html_merging"] = "正在归并" in page(p1)
        if not seen["waiting"] and any(st["reduce_waiting"].values()):
            seen["waiting"] = next(n for n in st["reduce_waiting"].values() if n)
            seen["html_waiting"] = "等待分片生成" in page(p1)

    st1 = wait_done(p1, on_status=watch1)
    rep1 = st1["report"]
    check("P1 正常完成且清单正确", rep1["still_5xx"] == expected,
          f"{len(rep1['still_5xx'])} vs {len(expected)}")
    check("观测到 reading 进度（文件+来源进程）",
          bool(seen["reading"]) and all(seen["reading"]),
          f"{seen['reading']}")
    check("任务页显示\"正在读 … 的分片（由 … 提供）\"", seen["html_reading"])
    check("观测到 merging（读齐后正在归并）", seen["merging"])
    check("任务页显示\"正在归并\"", seen["html_merging"])
    check("分片未齐时列出在等哪些文件", bool(seen["waiting"]),
          f"{seen['waiting']}")
    check("任务页显示\"等待分片生成：…\"", seen["html_waiting"])

    # --------------------------------------- B. 迟到旧进度 fencing + 已提交
    print("== 3. 任务 P2：掉线重派后迟到的旧进度被拒")
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    p2 = r["job_id"]

    def two_assigned():
        st = status(p2)
        a = {p: (rd["worker"], rd["epoch"])
             for p, rd in st["reduces"].items() if rd["state"] == "ASSIGNED"}
        return a if len(a) >= 2 else None

    snap = poll_until(two_assigned, timeout=60)
    check("两个进程同时处于归并中", bool(snap), f"{snap}")
    if not snap:
        return finish()
    st = status(p2)
    boots = {wid: w["boot_id"] for wid, w in st["workers"].items()}
    # 选一个 ASSIGNED 中的分区做"当前轮" fencing：未来 epoch / 错 boot / 错执行者
    p_any, (w_any, e_any) = next(iter(snap.items()))
    w_other = next(w for w in boots if w != w_any)
    rej = [post_progress(w_any, p2, int(p_any), e_any + 100, boots[w_any]),
           post_progress(w_any, p2, int(p_any), e_any, "wrong-boot"),
           post_progress(w_other, p2, int(p_any), e_any, boots[w_other])]
    check("当前轮的非法进度（未来 epoch/错 boot/错执行者）被拒",
          rej == [False, False, False], f"{rej}")
    pr_now = status(p2)["reduces"][p_any].get("progress") or {}
    check("非法进度没有覆盖当前进度", pr_now.get("file_id") != "STALE_SENTINEL")

    # 停掉持有分片的进程：其 reduce 重派，幸存者的 reduce 作废（epoch+1）
    holders = {}
    for fid, m in st["maps"].items():
        if m["worker"]:
            holders.setdefault(m["worker"], set()).add(fid)
    victim = next((w for w in sorted(holders) if w in boots), None)
    survivor = next(w for w in boots if w != victim)
    p_victim = next(p for p, (w, e) in snap.items() if w == victim)
    p_surv = next(p for p, (w, e) in snap.items() if w == survivor)
    e_victim, e_surv = snap[p_victim][1], snap[p_surv][1]
    http("POST", f"/api/jobs/{p2}/kill/{victim}", expect=200)

    def both_bumped():
        st = status(p2)
        return (st["reduces"][p_victim]["epoch"] > e_victim
                and st["reduces"][p_surv]["epoch"] > e_surv) or None

    check("掉线后受害分区与幸存者的分区都被重派（epoch+1）",
          bool(poll_until(both_bumped, timeout=15)))
    rej2 = [post_progress(victim, p2, int(p_victim), e_victim, boots[victim]),
            post_progress(survivor, p2, int(p_surv), e_surv, boots[survivor])]
    check("重派后迟到的旧进度（旧 epoch）被拒", rej2 == [False, False],
          f"{rej2}")
    st = status(p2)
    clean = all((st["reduces"][p].get("progress") or {}).get("file_id")
                != "STALE_SENTINEL" for p in (p_victim, p_surv))
    check("旧进度没有覆盖新一轮进度", clean)

    st2 = wait_done(p2)
    check("P2 掉线后仍完成且清单正确", st2["report"]["still_5xx"] == expected)
    ev = "\n".join(e["msg"] for e in st2["events"])
    check("事件流可见\"ignored late progress\"", "ignored late progress" in ev)
    rd0 = st2["reduces"]["0"]
    rej3 = post_progress(rd0["worker"] or victim, p2, 0, rd0["epoch"],
                         boots.get(rd0["worker"], "x"))
    check("已提交分区的迟到进度被拒", rej3 is False)
    st = status(p2)
    check("已提交分区始终显示完成（COMMITTED 不受进度影响）",
          all(rd["state"] == "COMMITTED" for rd in st["reduces"].values()))
    check("任务页分区列显示\"完成\"", "完成" in page(p2))

    # --------------------------- C. 拿不到进度：幽灵 worker 领了归并不上报
    print("== 4. 任务 P3：拿不到进度时提示暂时无法确认，报告仍照常产出")
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    p3 = r["job_id"]
    http("POST", "/api/workers/register",
         body={"worker_id": "ghost1", "boot_id": "ghostboot1",
               "base_url": "http://127.0.0.1:9"}, expect=200)
    # 等 map 全部完成后，幽灵去领一个 reduce，然后永远沉默
    poll_until(lambda: all(m["state"] == "DONE"
                           for m in status(p3)["maps"].values()) or None,
               timeout=60)
    ghost_task = poll_until(
        lambda: (lambda t: t if t and t.get("type") == "reduce" else None)(
            next_task("ghost1", "ghostboot1")),
        timeout=60)
    check("幽灵 worker 领到了一个归并分区", bool(ghost_task),
          f"partition={ghost_task and ghost_task['partition']}")
    gp = str(ghost_task["partition"])

    def ghost_held():
        rd = status(p3)["reduces"][gp]
        return (rd["state"] == "ASSIGNED" and rd["worker"] == "ghost1"
                and rd.get("progress") is None) or None

    check("分区已派给幽灵且拿不到任何进度",
          bool(poll_until(ghost_held, timeout=15)))
    check("任务页提示\"暂时无法确认\"", "暂时无法确认" in page(p3))
    st3 = wait_done(p3, timeout=180)  # 心跳超时后重派，由真实 worker 完成
    check("幽灵被心跳超时回收、分区重派后完成",
          st3["reduces"][gp]["state"] == "COMMITTED"
          and st3["reduces"][gp]["worker"] != "ghost1")
    check("拿不到进度归并仍照常产出正确报告",
          st3["report"]["still_5xx"] == expected)

    # ----------------- D. 读取失败：保留最后一次失败的文件和来源直到重试
    print("== 5. 任务 P4：读取失败保留文件+来源，直到分区重试")
    http("POST", "/api/workers/register",
         body={"worker_id": "ghost2", "boot_id": "ghostboot2",
               "base_url": "http://127.0.0.1:9"}, expect=200)
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    p4 = r["job_id"]
    # 幽灵抢一个 map 并提交"空气分片"（登记了但取不到），随后沉默
    gmap = poll_until(
        lambda: (lambda t: t if t and t.get("type") == "map" else None)(
            next_task("ghost2", "ghostboot2")),
        timeout=60)
    check("幽灵 worker 抢到了一个 map 任务", bool(gmap),
          f"file={gmap and gmap['file_id']}")
    http("POST", "/api/tasks/map-commit/ghost2",
         body={"job_id": p4, "file_id": gmap["file_id"], "epoch": gmap["epoch"],
               "boot_id": "ghostboot2",
               "shards": [{"part": p, "records": 0} for p in range(8)]},
         expect=200)
    gfile = gmap["file_id"]
    # 真实 worker 的归并读幽灵的分片 -> 读取失败 -> last_error 保留到重试
    err = poll_until(lambda: next(
        ((p, rd["last_error"]) for p, rd in status(p4)["reduces"].items()
         if rd.get("last_error")), None), timeout=60)
    check("读取失败的分区记录了失败的文件和来源", bool(err),
          f"{err and err[1]}")
    if err:
        p_err, le = err
        check("last_error 指向幽灵的文件与进程",
              le.get("file_id") == gfile and le.get("source") == "ghost2",
              f"{le}")
        html = page(p4)
        check("任务页显示\"上次读取失败\"及来源",
              "上次读取失败" in html and "ghost2" in html)

        def cleared():
            rd = status(p4)["reduces"][p_err]
            return (rd["state"] == "ASSIGNED"
                    and rd.get("last_error") is None) or None

        check("分区重试后 last_error 被清除",
              bool(poll_until(cleared, timeout=60)))
    st4 = wait_done(p4)
    check("P4 补算后完成且清单正确", st4["report"]["still_5xx"] == expected)
    check("完成后全部分区无遗留 last_error",
          all(not rd.get("last_error") for rd in st4["reduces"].values()))
    ev4 = "\n".join(e["msg"] for e in st4["events"])
    check("事件流可见读取失败与补算",
          "cannot fetch shard" in ev4 and "recompute scheduled" in ev4)

    return finish()


def finish() -> int:
    failed = [c for c in CHECKS if not c[1]]
    print(f"\n{'全部通过' if not failed else '存在失败'}: "
          f"{len(CHECKS) - len(failed)}/{len(CHECKS)} 项通过")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
