"""快照管理专用路由：读取增量发布状态记录核对可采集数据并固化快照清单。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.services import service_for

router = APIRouter(prefix="/api/snapshot_mgr", tags=["快照管理"])


def _service():
    service = service_for("snapshot_mgr")
    if service is None:
        raise HTTPException(status_code=404, detail="快照管理服务未注册")
    return service


@router.get("/table/{table}/manifest")
def table_publish_manifest(table: str) -> dict[str, Any]:
    """快照发布清单：仅已确认事务可采集，附带排除原因与可接续范围。"""
    return _service().publish_manifest(table)


@router.post("/{entry_id}/collect")
def collect_snapshot(entry_id: int) -> dict[str, Any]:
    """创建快照时按增量发布状态记录核对并固化可采集数据行。"""
    result = _service().domain_create_snapshot(entry_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "快照不存在"))
    return result
