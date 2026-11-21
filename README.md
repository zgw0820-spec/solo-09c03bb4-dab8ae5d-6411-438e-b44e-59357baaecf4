# 重试后仍 5xx 请求分析台

值班员上传多份代理日志（CSV），系统用两个本地工作进程做"按文件分片 →
按请求号归并"的分布式归并，找出**重试后仍报 5xx 的请求**。归并途中可以
从网页停掉一个工作进程：它丢失的中间分片由幸存进程从原文件补算，完好
分片继续复用；读过丢失分片、尚未完成的归并作废重跑，作废任务的迟到
结果不会进入报告。全部归并完成后才显示清单，每个请求以最新记录为准。

## 架构

```
        ┌────────────┐   共享卷 /data（上传、归并输出、状态）
        │ web (8000) │◀──────────────┐
        │ FastAPI    │               │
        │ 协调器+UI   │               │
        └─────┬──────┘               │
   任务派发/提交 │ 心跳                │
      ┌───────┴────────┐            │
      │                │            │
┌─────▼─────┐   ┌──────▼────┐       │
│ worker1   │◀──▶│ worker2   │  分片  │
│ :8101     │分片│ :8102     │  互拉  │
│ /shards   │HTTP│ /shards   │       │
│ (tmpfs)   │   │ (tmpfs)   │       │
└───────────┘   └───────────┘       │
```

- **Map**：worker 读一个 CSV，按 `md5(request_id) % 8` 分成 8 个中间分片，
  写到**自己本地**的 `/shards`（Docker 里是 tmpfs，进程停即失），再向协调器
  登记分片清单。
- **Reduce**：worker 领取一个分区，从分片清单找到全部输入（自己的本地读，
  别人的走 HTTP 拉取），按请求号归并、取最新记录，先写临时文件，再向
  协调器提交；协调器验收后才改名入库。
- **容错**：每个任务带 epoch。进程被停 → 其分片标记 LOST、对应文件重新
  派发补算；读过丢失分片且未提交的归并 epoch+1 作废重跑；作废任务的
  迟到提交因 epoch 不符被 409 拒绝并删除临时文件，**不进报告**。已提交
  的归并不重跑——map 是确定性的，补算出的分片与原来逐字节一致。
- **掉线检测**有三条路径：网页主动停（立即）、心跳超时（10 秒）、幸存
  进程拉取分片失败上报（协调器健康检查确认后标记）。

## 运行（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

1. 上传 CSV（可多选），列：`request_id,timestamp,status`
   （时间戳支持 epoch 秒或 ISO-8601；同一请求的时间戳互异）。
2. 勾选文件 → "开始分析" → 进入任务页（2 秒自动刷新）。
3. 归并途中点 **"停掉 workerX"**：事件流可见分片丢失、补算、归并作废
   重跑、迟到结果被拒；分片网格红变绿；完成后显示清单。
4. 恢复被停的进程：`docker compose start worker1`（新 boot id 注册，旧
   分片按丢失处理）。`docker compose down -v` 清空全部数据。

清单口径：**尝试次数 ≥ 2 且最新一条记录为 5xx** 的请求（"重试后仍 5xx"）。
只报 5xx 但未重试的请求单独计数、不入清单。

## 核对（同一批日志：正常归并 vs 掉线后）

```bash
# 服务已启动后（compose 或下方本地方式均可）：
python3 scripts/verify.py http://localhost:8000
```

脚本生成 4 份确定性日志（6000 请求 / 12813 条记录，含 ISO 时间戳与坏行），
先正常跑一遍（任务 A），再在归并途中停掉一个进程跑一遍（任务 B），断言：
A 清单 == 期望清单；B 清单 == A 清单；完成前 /report 返回 409；只补算掉线
进程的分片；作废归并的迟到提交被拒。另附事件回放。

## 本地开发（无 Docker）

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATA_DIR=/tmp/d .venv/bin/python -m uvicorn app.web.main:app --port 8000 &
WORKER_ID=worker1 WORKER_PORT=8101 WORKER_BASE_URL=http://localhost:8101 \
  COORDINATOR_URL=http://localhost:8000 DATA_DIR=/tmp/d SHARD_DIR=/tmp/s1 \
  REDUCE_DELAY_MS=500 .venv/bin/python -m app.worker.worker &
WORKER_ID=worker2 WORKER_PORT=8102 WORKER_BASE_URL=http://localhost:8102 \
  COORDINATOR_URL=http://localhost:8000 DATA_DIR=/tmp/d SHARD_DIR=/tmp/s2 \
  REDUCE_DELAY_MS=500 .venv/bin/python -m app.worker.worker &
```

`REDUCE_DELAY_MS` 模拟归并耗时，拉大"读到丢失分片但尚未提交"的窗口，
便于演示迟到提交被拒；生产可设为 0。

## 目录

```
app/common.py        CSV 解析、分片、归并（取最新记录）
app/web/main.py      协调器 + UI（任务派发、epoch fencing、报告）
app/web/store.py     JSON 状态持久化
app/web/templates/   页面模板
app/worker/worker.py 工作进程（map/reduce、分片服务、控制面）
scripts/gen_logs.py  确定性测试日志生成（含期望清单）
scripts/verify.py    端到端核对脚本
```

## 遗留事项

- 单协调器、状态存 JSON 文件，无高可用；生产应换数据库 + 选主。
- 无慢worker推测执行：只做故障重派，慢任务不会被并行兜底。
- 补算以"文件"为粒度（一个文件的分片全在同一进程）；更细粒度可按
  (文件, 分区) 重算，代价是 map 要支持按分区产出。
- 清单口径固定为"≥2 次尝试且最新 5xx"，未做可配置过滤/导出 CSV。
- 无鉴权，仅限内网演示；`/reset` 会清空全部数据。
- 已完成任务的分片网格在进程掉线后仍显示 OK（报告已生成，不受影响）。
