"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, chains, incremental_publish, snapshot_mgr

# 带固定子路径（/todo、/resolve、/manifest 等）的专用路由必须排在通配
# /{module} 之前，否则固定路径会被资源路由的动态段抢先匹配。
ROUTERS = [incremental_publish, snapshot_mgr, resource, chains]
