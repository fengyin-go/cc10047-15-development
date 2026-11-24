"""增量发布状态记录测试。

覆盖任务要求的全部语义：
* W8 备单（订单行）后撤销一次；
* W9 的确认与撤销并发抵达，只留下一个唯一终态；
* 重复确认幂等、不生成第二个版本；已撤销不能跳级回发布；
* 快照发布清单只采集已确认事务，读者只能在新快照看到确认过的数据行；
* 并发冲突与断线重放都回放同一个终态；
* 发布待办展示完成的操作、快照可读性与可接续数据范围。
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, '.')

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import service_for
from app.services.incremental_publish import (
    ACTION_ABORT,
    ACTION_CONFIRM,
    STATE_ABORTED,
    STATE_CONFIRMED,
    STATE_PENDING,
)
from app.store import store

client = TestClient(app)
MODULE = "incremental_publish"


@pytest.fixture
def publish_svc():
    """每个用例使用独立的增量发布表，避免种子数据和用例间互相污染。"""
    svc = service_for(MODULE)
    original = [dict(row) for row in store.rows(MODULE)]
    store.rows(MODULE).clear()
    yield svc
    store.rows(MODULE).clear()
    store.rows(MODULE).extend(original)


def _prepare(svc, txn_id: str, worker: str, rows: int, start: int, table: str = "orders_lake"):
    entry, missing = svc.create_entry({
        "事务标识": txn_id,
        "目标湖表": table,
        "写入批次": f"batch-{txn_id}",
        "操作工人": worker,
        "数据行数": rows,
        "起始偏移": start,
        "结束偏移": start + rows,
    })
    assert not missing
    assert entry["状态"] == STATE_PENDING
    return entry


class TestPrepareAndAbort:
    def test_w8_prepare_order_rows_then_abort(self, publish_svc):
        """W8 准备一批订单行并撤销一次：数据行不进快照，偏移仍可接续。"""
        entry = _prepare(publish_svc, "TXN-W8-1001", "W8", rows=120, start=0)
        assert entry["数据行数"] == 120
        assert entry["起始偏移"] == 0 and entry["结束偏移"] == 120

        result = publish_svc.resolve(entry["id"], ACTION_ABORT, worker="W8")
        assert result["ok"] is True
        assert result["conflict"] is False
        sealed = result["entry"]
        assert sealed["状态"] == STATE_ABORTED
        assert sealed["已发布版本"] is None
        assert sealed["终态操作"] == ACTION_ABORT
        assert sealed["终态工人"] == "W8"
        assert sealed["终态证据"]  # 终态证据必须留痕

        # 已撤销事务对快照不可读，但其偏移范围仍可接续重放。
        assert publish_svc.readable_rows("orders_lake") == 0
        rng = publish_svc.resumable_range("orders_lake")
        assert rng["resume_from_offset"] == 0
        assert "TXN-W8-1001" in rng["aborted_txn"]

    def test_duplicate_txn_id_rejected(self, publish_svc):
        """同一增量事务只能登记一次。"""
        _prepare(publish_svc, "TXN-DUP-1", "W8", rows=10, start=0)
        entry, missing = publish_svc.create_entry({
            "事务标识": "TXN-DUP-1", "目标湖表": "orders_lake",
            "写入批次": "b2", "操作工人": "W9",
        })
        assert entry is None
        assert missing  # 返回重复登记原因


class TestUniqueTerminalState:
    def test_w9_confirm_and_abort_concurrent(self, publish_svc):
        """W9 的确认与撤销并发抵达：只有一笔操作完成，终态唯一。"""
        entry = _prepare(publish_svc, "TXN-W9-2001", "W9", rows=200, start=120)
        outcomes: list[dict] = []
        barrier = threading.Barrier(2)

        def worker(action: str):
            barrier.wait()  # 尽量让两个请求同时进入临界区
            outcomes.append(publish_svc.resolve(entry["id"], action, worker="W9"))

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(worker, ACTION_CONFIRM)
            f2 = pool.submit(worker, ACTION_ABORT)
            f1.result()
            f2.result()

        assert len(outcomes) == 2
        winners = [o for o in outcomes if o["ok"]]
        losers = [o for o in outcomes if not o["ok"]]
        assert len(winners) == 1
        assert len(losers) == 1
        assert losers[0]["conflict"] is True

        sealed = store.find(MODULE, entry["id"])
        assert sealed["状态"] in (STATE_CONFIRMED, STATE_ABORTED)
        # 落败方的冲突必须留痕，但终态只能有一个。
        assert sealed["冲突次数"] == 1
        terminal_actions = [h for h in sealed["操作历史"] if h.get("result") == "terminal"]
        assert len(terminal_actions) == 1

    def test_repeated_confirm_does_not_create_second_version(self, publish_svc):
        """重复提交确认幂等返回，不生成第二个版本。"""
        entry = _prepare(publish_svc, "TXN-W9-2002", "W9", rows=50, start=0)
        first = publish_svc.resolve(entry["id"], ACTION_CONFIRM, worker="W9")
        assert first["ok"] is True
        version = first["entry"]["已发布版本"]
        assert version

        second = publish_svc.resolve(entry["id"], ACTION_CONFIRM, worker="W9")
        assert second["ok"] is False
        assert second["idempotent_replay"] is True
        assert second["conflict"] is False
        assert second["entry"]["已发布版本"] == version

        third = publish_svc.resolve(entry["id"], ACTION_CONFIRM, worker="W9")
        assert third["entry"]["已发布版本"] == version
        terminal = [h for h in third["entry"]["操作历史"] if h.get("result") == "terminal"]
        assert len(terminal) == 1  # 始终只有一次终态写入

    def test_aborted_cannot_flow_back_to_publish(self, publish_svc):
        """已撤销是终态，确认操作不能让它跳级流转回发布。"""
        entry = _prepare(publish_svc, "TXN-W9-2003", "W9", rows=30, start=0)
        publish_svc.resolve(entry["id"], ACTION_ABORT, worker="W9")

        retry = publish_svc.resolve(entry["id"], ACTION_CONFIRM, worker="W9")
        assert retry["ok"] is False
        assert retry["conflict"] is True
        sealed = store.find(MODULE, entry["id"])
        assert sealed["状态"] == STATE_ABORTED
        assert sealed["已发布版本"] is None

        # 再次撤销属于同操作重放，同样不改变终态。
        replay = publish_svc.resolve(entry["id"], ACTION_ABORT, worker="W9")
        assert replay["idempotent_replay"] is True
        assert replay["entry"]["状态"] == STATE_ABORTED

    def test_many_racers_leave_single_terminal(self, publish_svc):
        """大量并发确认/撤销（含断线重放）最终仍只有唯一终态。"""
        entry = _prepare(publish_svc, "TXN-W9-2004", "W9", rows=90, start=200)
        actions = [ACTION_CONFIRM, ACTION_ABORT] * 8

        def race(action: str):
            return publish_svc.resolve(entry["id"], action, worker="W9")

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(race, actions))

        winners = [r for r in results if r["ok"]]
        assert len(winners) == 1
        sealed = store.find(MODULE, entry["id"])
        terminal = [h for h in sealed["操作历史"] if h.get("result") == "terminal"]
        assert len(terminal) == 1
        assert sealed["状态"] in (STATE_CONFIRMED, STATE_ABORTED)
        if sealed["状态"] == STATE_CONFIRMED:
            # 确认胜出时版本号在同表内单调，且只分配一次。
            assert sealed["已发布版本"]
            assert winners[0]["entry"]["已发布版本"] == sealed["已发布版本"]


class TestSnapshotManifest:
    def test_manifest_only_collects_confirmed(self, publish_svc):
        """快照发布清单依据已确认事务选择可采集数据。"""
        _prepare(publish_svc, "TXN-W8-3001", "W8", rows=120, start=0)
        confirmed = _prepare(publish_svc, "TXN-W7-3002", "W7", rows=80, start=0)
        aborted = _prepare(publish_svc, "TXN-W7-3003", "W7", rows=40, start=80)
        publish_svc.resolve(confirmed["id"], ACTION_CONFIRM, worker="W7")
        publish_svc.resolve(aborted["id"], ACTION_ABORT, worker="W7")

        snap_svc = service_for("snapshot_mgr")
        manifest = snap_svc.publish_manifest("orders_lake")
        collectable_ids = [item["事务标识"] for item in manifest["collectable_txn"]]
        assert collectable_ids == ["TXN-W7-3002"]
        assert manifest["collectable_rows"] == 80
        excluded = {item["事务标识"]: item["状态"] for item in manifest["excluded_txn"]}
        assert excluded == {"TXN-W8-3001": STATE_PENDING, "TXN-W7-3003": STATE_ABORTED}

        # 读者在新快照里只能看到已确认事务所含的数据行。
        assert publish_svc.readable_rows("orders_lake") == 80

    def test_create_snapshot_seals_confirmed_rows(self, publish_svc):
        """创建快照时固化的行数只来自已确认事务。"""
        t1 = _prepare(publish_svc, "TXN-S-4001", "W8", rows=120, start=0)
        t2 = _prepare(publish_svc, "TXN-S-4002", "W9", rows=60, start=120)
        publish_svc.resolve(t2["id"], ACTION_CONFIRM, worker="W9")

        snap_svc = service_for("snapshot_mgr")
        snap_row, _ = snap_svc.create_entry({
            "快照标识": "SNAP-IT-1", "目标湖表": "orders_lake", "快照时间": "2026-11-24",
        })
        result = snap_svc.domain_create_snapshot(snap_row["id"])
        assert result["ok"] is True
        assert result["row_count"] == 60  # t1 待发布、t2 已确认
        assert snap_row["数据行数"] == 60
        assert snap_row["接续范围"]["resume_from_offset"] == 0  # 待发布 t1 仍可接续

        # W8 随后确认，新快照才能看到全部 180 行；旧快照清单不变。
        publish_svc.resolve(t1["id"], ACTION_CONFIRM, worker="W8")
        assert publish_svc.readable_rows("orders_lake") == 180
        assert snap_row["数据行数"] == 60

    def test_version_monotonic_per_table(self, publish_svc):
        """同表多次确认分配单调递增版本，不同表各自计数。"""
        a = _prepare(publish_svc, "TXN-V-5001", "W8", rows=10, start=0, table="orders_lake")
        b = _prepare(publish_svc, "TXN-V-5002", "W9", rows=20, start=10, table="orders_lake")
        c = _prepare(publish_svc, "TXN-V-5003", "W9", rows=5, start=0, table="payments_lake")
        ra = publish_svc.resolve(a["id"], ACTION_CONFIRM, worker="W8")
        rb = publish_svc.resolve(b["id"], ACTION_CONFIRM, worker="W9")
        rc = publish_svc.resolve(c["id"], ACTION_CONFIRM, worker="W9")
        assert ra["entry"]["已发布版本"] == "v-0001"
        assert rb["entry"]["已发布版本"] == "v-0002"
        assert rc["entry"]["已发布版本"] == "v-0001"


class TestResumableRange:
    def test_range_after_mixed_states(self, publish_svc):
        """已确认连续可见；待发布/已撤销占用的偏移仍可接续。"""
        c1 = _prepare(publish_svc, "TXN-R-6001", "W7", rows=80, start=0)
        a1 = _prepare(publish_svc, "TXN-R-6002", "W8", rows=40, start=80)
        p1 = _prepare(publish_svc, "TXN-R-6003", "W9", rows=60, start=120)
        publish_svc.resolve(c1["id"], ACTION_CONFIRM, worker="W7")
        publish_svc.resolve(a1["id"], ACTION_ABORT, worker="W8")

        rng = publish_svc.resumable_range("orders_lake")
        assert rng["confirmed_end_offset"] == 80
        assert rng["resume_from_offset"] == 80  # 从未确认事务的最小偏移接续
        assert rng["aborted_txn"] == ["TXN-R-6002"]
        assert rng["pending_txn"] == ["TXN-R-6003"]

    def test_range_all_confirmed(self, publish_svc):
        c1 = _prepare(publish_svc, "TXN-R-6004", "W7", rows=80, start=0)
        publish_svc.resolve(c1["id"], ACTION_CONFIRM, worker="W7")
        rng = publish_svc.resumable_range("orders_lake")
        assert rng["resume_from_offset"] == 80
        assert rng["pending_txn"] == [] and rng["aborted_txn"] == []


class TestPublishTodoAndApi:
    def test_todo_reports_winner_readability_and_range(self, publish_svc):
        """发布待办显示完成的操作、快照可读性与接续范围。"""
        w8 = _prepare(publish_svc, "TXN-T-7001", "W8", rows=120, start=0)
        publish_svc.resolve(w8["id"], ACTION_ABORT, worker="W8")
        w9 = _prepare(publish_svc, "TXN-T-7002", "W9", rows=200, start=120)

        # 并发确认与撤销，记录哪笔操作完成。
        barrier = threading.Barrier(2)
        outcome: dict[str, dict] = {}

        def race(action: str):
            barrier.wait()
            outcome[action] = publish_svc.resolve(w9["id"], action, worker="W9")

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(race, ACTION_CONFIRM)
            f2 = pool.submit(race, ACTION_ABORT)
            f1.result()
            f2.result()

        todo = publish_svc.publish_todo()
        assert todo["total"] == 2
        assert todo["terminal"] == 2
        assert todo["conflict_count"] == 1
        table_view = next(t for t in todo["tables"] if t["table"] == "orders_lake")
        winning_action = next(
            action for action, result in outcome.items() if result["ok"]
        )
        w9_row = next(item for item in todo["items"] if item["事务标识"] == "TXN-T-7002")
        assert w9_row["终态操作"] == winning_action  # 待办显示哪笔操作完成
        if winning_action == ACTION_CONFIRM:
            assert table_view["snapshot_readable"] is True
            assert table_view["readable_rows"] == 200
        else:
            assert table_view["snapshot_readable"] is False
            assert table_view["readable_rows"] == 0
        # 撤销与落败事务的偏移仍可接续。
        assert table_view["resumable"]["resume_from_offset"] == 0

    def test_resolve_endpoint_conflict_and_replay(self, publish_svc):
        """HTTP 层：并发请求收敛唯一终态，重复请求幂等回放。"""
        entry = _prepare(publish_svc, "TXN-API-8001", "W9", rows=45, start=0)

        def call(action: str):
            return client.post(
                f"/api/{MODULE}/{entry['id']}/resolve",
                json={"action": action, "worker": "W9"},
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(call, [ACTION_CONFIRM, ACTION_ABORT]))

        assert all(resp.status_code == 200 for resp in responses)
        bodies = [resp.json() for resp in responses]
        assert sum(1 for body in bodies if body["ok"]) == 1
        assert sum(1 for body in bodies if body["conflict"]) == 1

        # 断线恢复：用相同动作再次提交，回放既有终态而非新建版本。
        sealed = store.find(MODULE, entry["id"])
        replay = client.post(
            f"/api/{MODULE}/{entry['id']}/resolve",
            json={"action": sealed["终态操作"], "worker": "W9"},
        ).json()
        assert replay["idempotent_replay"] is True
        assert replay["entry"]["状态"] == sealed["状态"]
        assert replay["entry"]["终态证据"] == sealed["终态证据"]

    def test_detail_and_manifest_endpoints(self, publish_svc):
        """事务详情与快照清单端点给出可读行、终态证据和接续范围。"""
        entry = _prepare(publish_svc, "TXN-API-8002", "W8", rows=120, start=0)
        client.post(
            f"/api/{MODULE}/{entry['id']}/resolve",
            json={"action": ACTION_ABORT, "worker": "W8"},
        )
        detail = client.get(f"/api/{MODULE}/{entry['id']}/detail").json()
        assert detail["terminal"] is True
        assert detail["terminal_evidence"]["state"] == STATE_ABORTED
        assert detail["terminal_evidence"]["evidence"]
        assert detail["readable_rows"] == 0
        assert detail["resumable"]["resume_from_offset"] == 0
        assert len(detail["operation_history"]) == 2

        manifest = client.get(f"/api/{MODULE}/table/orders_lake/manifest").json()
        assert manifest["confirmed"] == []
        assert manifest["readable_rows"] == 0

        todo = client.get(f"/api/{MODULE}/todo").json()
        assert todo["items"][0]["事务标识"] == "TXN-API-8002"

    def test_unknown_txn_and_illegal_action(self, publish_svc):
        resp = client.post(
            f"/api/{MODULE}/9999/resolve", json={"action": ACTION_CONFIRM, "worker": "W9"},
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False

        entry = _prepare(publish_svc, "TXN-API-8003", "W9", rows=5, start=0)
        bad = publish_svc.resolve(entry["id"], "删除表", worker="W9")
        assert bad["ok"] is False
        assert store.find(MODULE, entry["id"])["状态"] == STATE_PENDING
