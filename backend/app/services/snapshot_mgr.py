"""快照管理领域服务。

快照发布清单不再直接信任写入批次，而是读取增量发布状态记录，只采集
「已确认」事务所含的数据行；待发布与已撤销事务对快照读者不可见。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.incremental_publish import STATE_CONFIRMED
from app.store import store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "表快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["快照标识", "目标湖表", "快照时间", "数据行数", "存储大小", "过期时间", "快照标签", "保留状态"]
    REQUIRED_FIELDS = ["快照标识", "目标湖表", "快照时间"]
    STATUS_ORDER = ["创建中", "可用", "已过期", "已清理"]
    ACTION_RULES = {"创建快照": "可用", "延长保留": "已过期"}
    NEGATIVE_ACTIONS = []

    def _publish_service(self):
        from app.services import service_for
        return service_for("incremental_publish")

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def publish_manifest(self, table: str) -> dict[str, Any]:
        """快照发布清单：依据增量发布状态记录核对可采集数据。

        只有已确认事务进入 manifest；待发布/已撤销事务被排除并单独列出，
        供快照管理在刷新时核对「为什么这批行还不可读」。
        """
        publish_svc = self._publish_service()
        all_rows = [
            row for row in store.rows(publish_svc.MODULE)
            if str(row.get("目标湖表")) == table
        ]
        collectable = publish_svc.confirmed_manifest(table)
        excluded = [
            {
                "事务标识": row.get("事务标识"),
                "状态": row.get("状态"),
                "数据行数": self._safe_int(row.get("数据行数")),
                "起始偏移": self._safe_int(row.get("起始偏移")),
                "结束偏移": self._safe_int(row.get("结束偏移")),
            }
            for row in all_rows
            if row.get("状态") != STATE_CONFIRMED
        ]
        manifest_rows = sum(self._safe_int(row.get("数据行数")) for row in collectable)
        return {
            "table": table,
            "collectable_txn": [
                {
                    "事务标识": row.get("事务标识"),
                    "已发布版本": row.get("已发布版本"),
                    "数据行数": self._safe_int(row.get("数据行数")),
                    "起始偏移": self._safe_int(row.get("起始偏移")),
                    "结束偏移": self._safe_int(row.get("结束偏移")),
                    "终态证据": row.get("终态证据"),
                }
                for row in collectable
            ],
            "collectable_rows": manifest_rows,
            "excluded_txn": excluded,
            "resumable": publish_svc.resumable_range(table),
        }

    def domain_create_snapshot(self, entry_id: int) -> dict[str, Any]:
        """创建快照：读取增量发布状态记录核对可采集数据后再固化清单。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"快照 {entry_id} 不存在"}

        table = str(entry.get("目标湖表"))
        manifest = self.publish_manifest(table)
        # 快照可见行数以已确认事务为准，读者只能在新快照看到确认过的数据行。
        row_count = manifest["collectable_rows"]
        entry["数据行数"] = row_count
        entry["发布清单"] = manifest["collectable_txn"]
        entry["排除事务"] = manifest["excluded_txn"]
        entry["接续范围"] = manifest["resumable"]

        return {
            "ok": True,
            "snapshot_id": entry.get("快照标识"),
            "table": table,
            "snapshot_time": entry.get("快照时间"),
            "row_count": row_count,
            "storage_size_mb": self._safe_int(entry.get("存储大小"), 256),
            "expiration": entry.get("过期时间", "30d"),
            "label": entry.get("快照标签", "manual"),
            "manifest": manifest,
        }
