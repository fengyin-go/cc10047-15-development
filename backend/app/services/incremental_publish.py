"""增量发布领域服务。

补齐「增量写入批次 → 版本可见」之间缺失的发布状态记录：

* 增量事务先以「待发布」落库，版本在确认之前不可见，期间允许撤销；
* 同一事务只能收敛到一个终态（已确认 / 已撤销），由进程内互斥锁 +
  状态条件双重保证，确认与撤销并发抵达时只有一笔操作能完成终态写入；
* 重复确认是幂等的：不会生成第二个版本，直接回放第一次确认的终态证据；
* 已撤销是终态，不能跳级流转回发布；
* 断线重放（客户端重试同一笔操作）时返回既有终态与首次完成的操作，
  保证冲突和恢复都留下同一个唯一终态。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 增量发布状态机：待发布是唯一的中间态，其余两个均为终态。
STATE_PENDING = "待发布"
STATE_CONFIRMED = "已确认"
STATE_ABORTED = "已撤销"
TERMINAL_STATES = (STATE_CONFIRMED, STATE_ABORTED)

ACTION_CONFIRM = "确认发布"
ACTION_ABORT = "撤销发布"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class IncrementalPublishService(BaseModuleService):
    MODULE = "incremental_publish"
    ENTITY = "增量事务"
    OPERATION_TERM = "发布"
    LIST_FIELDS = [
        "事务标识", "目标湖表", "写入批次", "操作工人", "数据行数",
        "起始偏移", "结束偏移", "待发布版本",
    ]
    REQUIRED_FIELDS = ["事务标识", "目标湖表", "写入批次", "操作工人"]
    STATUS_ORDER = [STATE_PENDING, STATE_CONFIRMED, STATE_ABORTED]
    # 通用动作按钮仍登记在案；真正的流转规则在 run_action 内按状态机判定。
    ACTION_RULES = {ACTION_CONFIRM: STATE_CONFIRMED, ACTION_ABORT: STATE_ABORTED}
    NEGATIVE_ACTIONS = [ACTION_ABORT]

    # 同一进程内所有增量事务共享一把锁，模拟真实系统里按事务号串行化的
    # 行级锁/乐观 CAS；锁内只做内存状态判定与写入，临界区保持极短。
    _state_lock = threading.RLock()

    # ------------------------------------------------------------------
    # 登记 / 查询
    # ------------------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing
        txn_id = str(values.get("事务标识") or "").strip()
        with self._state_lock:
            existed = next(
                (row for row in store.rows(self.MODULE)
                 if str(row.get("事务标识")) == txn_id and row["id"] != entry["id"]),
                None,
            )
            if existed is not None:
                store.rows(self.MODULE).remove(entry)
                return None, [f"事务标识 {txn_id} 已存在，同一增量事务只能登记一次"]
            # 发布状态记录的明细字段：可采集数据行与可接续偏移范围。
            entry["状态"] = STATE_PENDING
            entry["数据行数"] = self._safe_int(values.get("数据行数"), 0)
            entry["起始偏移"] = self._safe_int(values.get("起始偏移"), 0)
            entry["结束偏移"] = self._safe_int(
                values.get("结束偏移"), entry["起始偏移"] + max(entry["数据行数"], 0)
            )
            entry["待发布版本"] = values.get("待发布版本") or f"v-pending-{entry['id']:04d}"
            entry["已发布版本"] = None
            entry["终态操作"] = None
            entry["终态工人"] = None
            entry["终态时间"] = None
            entry["终态证据"] = None
            entry["冲突次数"] = 0
            entry["操作历史"] = [
                {"action": "登记待发布", "worker": entry["操作工人"], "time": _now_iso(), "result": "accepted"}
            ]
            entry["pending"] = True
            entry["abnormal"] = False
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """通用动作入口：确认/撤销统一走带终态保护的状态机。"""
        if action not in (ACTION_CONFIRM, ACTION_ABORT):
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        result = self.resolve(entry_id, action, worker=None)
        if not result["ok"]:
            return None, result["message"]
        return result["entry"], result["message"]

    # ------------------------------------------------------------------
    # 核心：终态解析（确认 / 撤销 / 冲突 / 断线恢复共用同一条串行路径）
    # ------------------------------------------------------------------
    def resolve(self, entry_id: int, action: str, worker: str | None = None) -> dict[str, Any]:
        """对一笔增量事务提交确认或撤销，返回结构化终态结果。

        返回体固定包含：
        * ok / message：本次调用是否真正完成了终态写入；
        * conflict：是否与另一笔操作发生终态竞争；
        * idempotent_replay：是否为终态后的重复提交（断线重放）；
        * entry：最新发布状态记录（含终态证据、可接续范围）。
        """
        if action not in (ACTION_CONFIRM, ACTION_ABORT):
            return {"ok": False, "conflict": False, "idempotent_replay": False,
                    "message": f"动作「{action}」不属于增量发布可执行范围", "entry": None}

        with self._state_lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return {"ok": False, "conflict": False, "idempotent_replay": False,
                        "message": f"增量事务 {entry_id} 不存在或已归档", "entry": None}

            current = entry.get("状态", STATE_PENDING)
            history: list[dict[str, Any]] = entry.setdefault("操作历史", [])

            # 终态之后到达的任何操作（并发落败方、客户端断线重试）都不允许
            # 改写状态：回放首次终态，并区分「同操作重放」与「异操作冲突」。
            if current in TERMINAL_STATES:
                same_action = entry.get("终态操作") == action
                if not same_action:
                    entry["冲突次数"] = int(entry.get("冲突次数", 0)) + 1
                    history.append({
                        "action": action, "worker": worker, "time": _now_iso(),
                        "result": f"rejected: 终态已为{current}",
                    })
                return self._replay_result(
                    entry,
                    conflict=not same_action,
                    idempotent_replay=same_action,
                )

            # 仅「待发布」可以流转，且一次锁定后直接写入唯一终态。
            if action == ACTION_CONFIRM:
                return self._do_confirm(entry, worker, history)
            return self._do_abort(entry, worker, history)

    def _do_confirm(
        self, entry: dict[str, Any], worker: str | None, history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        version = self._allocate_version(entry)
        entry["状态"] = STATE_CONFIRMED
        entry["status"] = STATE_CONFIRMED
        entry["已发布版本"] = version
        self._seal_terminal(entry, ACTION_CONFIRM, worker, history, extra={"published_version": version})
        return {
            "ok": True, "conflict": False, "idempotent_replay": False,
            "message": f"增量事务 {entry['事务标识']} 已确认，发布版本 {version}，数据行已进入新快照可见范围",
            "entry": entry,
        }

    def _do_abort(
        self, entry: dict[str, Any], worker: str | None, history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        entry["状态"] = STATE_ABORTED
        entry["status"] = STATE_ABORTED
        entry["已发布版本"] = None
        self._seal_terminal(entry, ACTION_ABORT, worker, history, extra={"published_version": None})
        return {
            "ok": True, "conflict": False, "idempotent_replay": False,
            "message": f"增量事务 {entry['事务标识']} 已撤销，数据行不进入任何快照，偏移范围仍可接续重放",
            "entry": entry,
        }

    def _seal_terminal(
        self,
        entry: dict[str, Any],
        action: str,
        worker: str | None,
        history: list[dict[str, Any]],
        extra: dict[str, Any],
    ) -> None:
        sealed_at = _now_iso()
        evidence = self._evidence(entry, action, sealed_at)
        entry["终态操作"] = action
        entry["终态工人"] = worker or entry.get("操作工人")
        entry["终态时间"] = sealed_at
        entry["终态证据"] = evidence
        entry["pending"] = False
        # 撤销属于异常留痕，确认属于正常收敛。
        entry["abnormal"] = action == ACTION_ABORT
        history.append({
            "action": action, "worker": entry["终态工人"], "time": sealed_at,
            "result": "terminal", **extra,
        })

    def _replay_result(
        self, entry: dict[str, Any], *, conflict: bool, idempotent_replay: bool,
    ) -> dict[str, Any]:
        if conflict:
            message = (
                f"增量事务 {entry['事务标识']} 已存在唯一终态「{entry['状态']}」"
                f"（由{entry.get('终态工人')}的{entry.get('终态操作')}完成），本次操作被拒绝，终态不变"
            )
        else:
            message = (
                f"增量事务 {entry['事务标识']} 已{entry.get('终态操作')}，"
                f"重复提交幂等返回既有终态，不生成第二个版本"
            )
        return {
            "ok": False, "conflict": conflict,
            "idempotent_replay": idempotent_replay,
            "message": message, "entry": entry,
        }

    # ------------------------------------------------------------------
    # 版本分配：同表版本号单调递增，重复确认不会再次取号
    # ------------------------------------------------------------------
    def _allocate_version(self, entry: dict[str, Any]) -> str:
        table = str(entry.get("目标湖表"))
        taken = [
            int(str(row.get("已发布版本")).removeprefix("v-"))
            for row in store.rows(self.MODULE)
            if str(row.get("目标湖表")) == table
            and row.get("已发布版本")
            and str(row.get("已发布版本")).removeprefix("v-").isdigit()
        ]
        next_seq = (max(taken) + 1) if taken else 1
        return f"v-{next_seq:04d}"

    def _evidence(self, entry: dict[str, Any], action: str, sealed_at: str) -> str:
        material = "|".join([
            str(entry.get("事务标识")), str(entry.get("目标湖表")),
            str(entry.get("起始偏移")), str(entry.get("结束偏移")),
            str(entry.get("数据行数")), action, sealed_at,
        ])
        import hashlib
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

    # ------------------------------------------------------------------
    # 快照联动：只暴露已确认事务，按表/快照时间给出可采集清单
    # ------------------------------------------------------------------
    def confirmed_manifest(self, table: str | None = None) -> list[dict[str, Any]]:
        """快照发布清单：仅已确认事务可被快照采集。"""
        with self._state_lock:
            rows = [
                dict(row) for row in store.rows(self.MODULE)
                if row.get("状态") == STATE_CONFIRMED
                and (table is None or str(row.get("目标湖表")) == table)
            ]
        rows.sort(key=lambda row: int(row.get("起始偏移", 0)))
        return rows

    def readable_rows(self, table: str) -> int:
        """读者在新快照中能看到的行数：只统计已确认事务包含的数据行。"""
        return sum(int(row.get("数据行数", 0)) for row in self.confirmed_manifest(table))

    def resumable_range(self, table: str) -> dict[str, Any]:
        """后续还能接续的数据范围。

        已确认事务的偏移连续可见；待发布与已撤销事务占用过的偏移仍可重放，
        接续起点取所有事务里最小的未确认起始偏移，避免撤销后丢数据。
        """
        with self._state_lock:
            table_rows = [
                row for row in store.rows(self.MODULE)
                if str(row.get("目标湖表")) == table
            ]
            confirmed = [row for row in table_rows if row.get("状态") == STATE_CONFIRMED]
            unsettled = [row for row in table_rows if row.get("状态") != STATE_CONFIRMED]
        confirmed_end = max((int(row.get("结束偏移", 0)) for row in confirmed), default=0)
        if unsettled:
            resume_from = min(int(row.get("起始偏移", 0)) for row in unsettled)
        else:
            resume_from = confirmed_end
        return {
            "table": table,
            "confirmed_end_offset": confirmed_end,
            "resume_from_offset": resume_from,
            "pending_txn": [row.get("事务标识") for row in unsettled if row.get("状态") == STATE_PENDING],
            "aborted_txn": [row.get("事务标识") for row in unsettled if row.get("状态") == STATE_ABORTED],
        }

    # ------------------------------------------------------------------
    # 发布待办：列出终态完成情况、快照可读性与接续范围
    # ------------------------------------------------------------------
    def publish_todo(self) -> dict[str, Any]:
        with self._state_lock:
            rows = [dict(row) for row in store.rows(self.MODULE)]
        tables = sorted({str(row.get("目标湖表")) for row in rows})
        per_table = []
        for table in tables:
            table_rows = [row for row in rows if str(row.get("目标湖表")) == table]
            readable = self.readable_rows(table)
            per_table.append({
                "table": table,
                "pending": sum(1 for row in table_rows if row.get("状态") == STATE_PENDING),
                "confirmed": sum(1 for row in table_rows if row.get("状态") == STATE_CONFIRMED),
                "aborted": sum(1 for row in table_rows if row.get("状态") == STATE_ABORTED),
                "readable_rows": readable,
                "snapshot_readable": readable > 0,
                "resumable": self.resumable_range(table),
            })
        return {
            "total": len(rows),
            "pending": sum(1 for row in rows if row.get("状态") == STATE_PENDING),
            "terminal": sum(1 for row in rows if row.get("状态") in TERMINAL_STATES),
            "conflict_count": sum(int(row.get("冲突次数", 0)) for row in rows),
            "tables": per_table,
            "items": sorted(rows, key=lambda row: int(row.get("id", 0))),
        }

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("状态") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "增量发布状态记录",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }
