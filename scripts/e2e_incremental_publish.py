#!/usr/bin/env python3
"""增量发布状态记录端到端场景（W8/W9）。

流程严格对应题目：
1. W8 准备一批订单行并撤销一次；
2. W9 的确认与撤销并发抵达，核对唯一终态与冲突留痕；
3. 刷新快照列表、进入事务详情，核对可读行、终态证据与仍可接续范围；
4. 重复确认幂等、已撤销不可回流发布。

脚本自行拉起后端 uvicorn，全部走真实 HTTP 接口，结束后关闭服务。
运行：python3 -I scripts/e2e_incremental_publish.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BASE = "http://127.0.0.1:8011"
BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((name, bool(condition), detail))
    print(f"[{PASS if condition else FAIL}] {name}" + (f" — {detail}" if detail else ""))


def request(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_for_health(proc: subprocess.Popen) -> None:
    for _ in range(60):
        try:
            health = request("GET", "/api/health")
            if health.get("ok"):
                return
        except (urllib.error.URLError, ConnectionError):
            time.sleep(0.5)
    proc.kill()
    raise RuntimeError("后端健康检查超时")


def main() -> int:
    proc = subprocess.Popen(
        ["./.venv/bin/uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8011"],
        cwd=BACKEND_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_health(proc)
        print("== 后端已启动，开始 W8/W9 增量发布场景 ==\n")

        # ---------- W8：准备一批订单行 ----------
        w8 = request("POST", "/api/incremental_publish", {
            "values": {
                "事务标识": "TXN-W8-E2E-01", "目标湖表": "e2e_orders_lake",
                "写入批次": "batch-w8-e2e", "操作工人": "W8",
                "数据行数": 120, "起始偏移": 0, "结束偏移": 120,
            },
        })
        check("W8 备单成功", w8["ok"], w8["entry"]["事务标识"])
        w8_id = w8["entry"]["id"]
        check("备单初始为待发布（版本不可见）", w8["entry"]["状态"] == "待发布")

        # ---------- W8 撤销一次 ----------
        abort_w8 = request("POST", f"/api/incremental_publish/{w8_id}/resolve",
                           {"action": "撤销发布", "worker": "W8"})
        check("W8 撤销完成", abort_w8["ok"] and abort_w8["entry"]["状态"] == "已撤销",
              f"终态证据={abort_w8['entry']['终态证据']}")
        check("撤销不产生发布版本", abort_w8["entry"]["已发布版本"] is None)

        # 已撤销不能跳级流转回发布
        backfire = request("POST", f"/api/incremental_publish/{w8_id}/resolve",
                           {"action": "确认发布", "worker": "W8"})
        check("已撤销不可回流发布", (not backfire["ok"]) and backfire["conflict"]
              and backfire["entry"]["状态"] == "已撤销", backfire["message"])

        # ---------- W9：确认与撤销并发抵达 ----------
        w9 = request("POST", "/api/incremental_publish", {
            "values": {
                "事务标识": "TXN-W9-E2E-02", "目标湖表": "e2e_orders_lake",
                "写入批次": "batch-w9-e2e", "操作工人": "W9",
                "数据行数": 200, "起始偏移": 120, "结束偏移": 320,
            },
        })
        w9_id = w9["entry"]["id"]
        barrier = threading.Barrier(2)

        def concurrent_resolve(action: str) -> dict:
            barrier.wait()
            return request("POST", f"/api/incremental_publish/{w9_id}/resolve",
                           {"action": action, "worker": "W9"})

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_confirm = pool.submit(concurrent_resolve, "确认发布")
            f_abort = pool.submit(concurrent_resolve, "撤销发布")
            r_confirm, r_abort = f_confirm.result(), f_abort.result()

        outcomes = [r_confirm, r_abort]
        winners = [r for r in outcomes if r["ok"]]
        conflicts = [r for r in outcomes if r["conflict"]]
        check("并发下仅一笔操作完成", len(winners) == 1,
              f"完成方={winners[0]['requested_action']}")
        check("落败方记录为冲突", len(conflicts) == 1, conflicts[0]["message"])

        detail = request("GET", f"/api/incremental_publish/{w9_id}/detail")
        winning_action = detail["terminal_evidence"]["action"]
        check("事务详情呈现唯一终态", detail["terminal"] and detail["terminal_evidence"]["evidence"],
              f"状态={detail['terminal_evidence']['state']} 操作={winning_action}")
        check("冲突在终态记录上留痕", detail["terminal_evidence"]["conflict_count"] == 1)

        # 幂等重放：用胜出动作再次提交（模拟断线重试），不生成第二个版本；
        # 若胜出动作是撤销，则再提交确认必须作为异操作冲突被拒绝，终态不变。
        replay = request("POST", f"/api/incremental_publish/{w9_id}/resolve",
                         {"action": winning_action, "worker": "W9"})
        detail_after_replay = request("GET", f"/api/incremental_publish/{w9_id}/detail")
        version_after = detail_after_replay["terminal_evidence"]["published_version"]
        check("重复提交幂等回放、不生成第二版本",
              (not replay["ok"]) and replay["idempotent_replay"]
              and replay["entry"]["终态证据"] == detail["terminal_evidence"]["evidence"]
              and detail_after_replay["terminal_evidence"]["evidence"]
              == detail["terminal_evidence"]["evidence"],
              f"终态={detail_after_replay['terminal_evidence']['state']} 版本保持={version_after}")

        # 异操作重放：已确认再撤销 / 已撤销再确认，都不能改写唯一终态。
        opposite = "撤销发布" if winning_action == "确认发布" else "确认发布"
        cross = request("POST", f"/api/incremental_publish/{w9_id}/resolve",
                        {"action": opposite, "worker": "W9"})
        check("异操作不能改写唯一终态（不跳级流转）",
              (not cross["ok"]) and cross["conflict"]
              and cross["entry"]["状态"] == detail["terminal_evidence"]["state"],
              cross["message"])

        # ---------- 刷新快照列表并核对可采集数据 ----------
        snap_list = request("GET", "/api/snapshot_mgr")
        check("快照列表可刷新", snap_list["total"] >= 1)

        snap = request("POST", "/api/snapshot_mgr", {
            "values": {
                "快照标识": "SNAP-E2E-W9", "目标湖表": "e2e_orders_lake",
                "快照时间": "2026-11-24T10:00:00Z",
            },
        })
        snap_id = snap["entry"]["id"]
        collected = request("POST", f"/api/snapshot_mgr/{snap_id}/collect")
        manifest = request("GET", "/api/snapshot_mgr/table/e2e_orders_lake/manifest")

        expected_rows = 200 if winning_action == "确认发布" else 0
        check("快照只采集已确认事务的数据行",
              collected["row_count"] == expected_rows
              and manifest["collectable_rows"] == expected_rows,
              f"可读行={expected_rows}")
        excluded_ids = {item["事务标识"] for item in manifest["excluded_txn"]}
        check("待发布/已撤销事务被排除出快照清单",
              "TXN-W8-E2E-01" in excluded_ids,
              f"排除事务={sorted(excluded_ids)}")

        # ---------- 发布待办：完成情况、可读性、接续范围 ----------
        todo = request("GET", "/api/incremental_publish/todo")
        table_view = next(t for t in todo["tables"] if t["table"] == "e2e_orders_lake")
        check("发布待办显示哪笔操作完成",
              any(item["事务标识"] == "TXN-W9-E2E-02" and item["终态操作"] == winning_action
                  for item in todo["items"]),
              f"W9 终态操作={winning_action}")
        check("发布待办显示快照当前是否可读",
              table_view["snapshot_readable"] == (expected_rows > 0)
              and table_view["readable_rows"] == expected_rows)
        check("发布待办显示后续可接续范围",
              table_view["resumable"]["resume_from_offset"] == 0
              and "TXN-W8-E2E-01" in table_view["resumable"]["aborted_txn"],
              f"接续偏移={table_view['resumable']['resume_from_offset']}")

        # 事务详情内的可读行、终态证据、接续范围三项核对
        detail2 = request("GET", f"/api/incremental_publish/{w9_id}/detail")
        check("事务详情可读行与快照一致", detail2["readable_rows"] == expected_rows)
        check("事务详情含终态证据", bool(detail2["terminal_evidence"]["evidence"])
              and detail2["terminal_evidence"]["time"])
        expected_aborted = (
            ["TXN-W8-E2E-01"] if winning_action == "确认发布"
            else ["TXN-W8-E2E-01", "TXN-W9-E2E-02"]
        )
        check("事务详情含可接续范围",
              detail2["resumable"]["resume_from_offset"] == 0
              and detail2["resumable"]["aborted_txn"] == expected_aborted,
              f"aborted_txn={detail2['resumable']['aborted_txn']}")
        check("操作历史包含登记、终态与冲突三类留痕",
              len(detail2["operation_history"]) >= 3)

        print("\n== 场景结束 ==")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n汇总：{len(results) - len(failed)}/{len(results)} 项核对通过")
    if failed:
        print("失败项：" + "、".join(failed))
        return 1
    print("全部核对通过：唯一终态、冲突留痕、幂等与快照可见性语义均成立。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
