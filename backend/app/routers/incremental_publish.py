"""增量发布专用路由。

通用资源路由负责登记/列表/普通动作；这里承载带冲突语义的终态解析、
发布待办、事务详情与快照发布清单查询，状态流转仍全部落在领域服务内。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services import service_for
from app.services.incremental_publish import ACTION_ABORT, ACTION_CONFIRM

router = APIRouter(prefix="/api/incremental_publish", tags=["增量发布"])


class ResolvePayload(BaseModel):
    action: str
    worker: str | None = None
    remark: str | None = None


def _service():
    service = service_for("incremental_publish")
    if service is None:
        raise HTTPException(status_code=404, detail="增量发布服务未注册")
    return service


@router.post("/{entry_id}/resolve")
def resolve_publish(entry_id: int, payload: ResolvePayload) -> dict[str, Any]:
    """确认或撤销一笔增量事务；并发冲突与断线重放由服务内状态机收敛。"""
    service = _service()
    result = service.resolve(entry_id, payload.action.strip(), worker=payload.worker)
    # 冲突/终态重放属于业务可预期结果，保留 200 让发布待办能展示分歧。
    result["requested_action"] = payload.action
    result["worker"] = payload.worker
    return result


@router.get("/todo")
def publish_todo() -> dict[str, Any]:
    """发布待办：哪笔操作完成、快照是否可读、后续可接续的数据范围。"""
    return _service().publish_todo()


@router.get("/{entry_id}/detail")
def transaction_detail(entry_id: int) -> dict[str, Any]:
    """事务详情：终态证据、可读行核对与可接续范围。"""
    service = _service()
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"增量事务 {entry_id} 不存在或已归档")
    table = str(entry.get("目标湖表"))
    manifest = service.confirmed_manifest(table)
    return {
        "entry": entry,
        "table": table,
        "terminal": entry.get("状态") in ("已确认", "已撤销"),
        "terminal_evidence": {
            "state": entry.get("状态"),
            "action": entry.get("终态操作"),
            "worker": entry.get("终态工人"),
            "time": entry.get("终态时间"),
            "evidence": entry.get("终态证据"),
            "published_version": entry.get("已发布版本"),
            "conflict_count": entry.get("冲突次数", 0),
        },
        "readable_rows": service.readable_rows(table),
        "resumable": service.resumable_range(table),
        "confirmed_manifest": manifest,
        "operation_history": entry.get("操作历史", []),
    }


@router.get("/table/{table}/manifest")
def table_manifest(table: str) -> dict[str, Any]:
    """按表查看快照可采集清单（仅已确认事务）。"""
    service = _service()
    return {
        "table": table,
        "confirmed": service.confirmed_manifest(table),
        "readable_rows": service.readable_rows(table),
        "resumable": service.resumable_range(table),
    }


@router.get("/actions/options")
def action_options() -> dict[str, Any]:
    """暴露合法终态动作，供前端按钮与校验复用。"""
    return {"actions": [ACTION_CONFIRM, ACTION_ABORT]}
