from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def data_root(monkeypatch):
    tmp = Path(tempfile.mkdtemp(prefix="segment-version-case-"))
    import app.services.base as base
    monkeypatch.setattr(base, "DATA_ROOT", tmp)
    for sub in ["track_segments", "route_restore", "checkpoints"]:
        (tmp / sub).mkdir(parents=True, exist_ok=True)
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


def _seg(ref, terminal, start, end, *, source_ids=None, **extra):
    row = {
        "segment_id": ref,
        "segment_code": ref,
        "batch_key": "CASE-B",
        "terminal": terminal,
        "start_time": start,
        "end_time": end,
        "point_count": 6,
        "source_point_ids": source_ids or [ref],
        "start_lat": 39.90 + (start % 5) * 0.001,
        "start_lon": 116.40 + (start % 7) * 0.001,
        "end_lat": 39.91 + (end % 5) * 0.001,
        "end_lon": 116.41 + (end % 7) * 0.001,
        "mileage": 120.0,
        "status": "已确认",
        "confirmed": True,
        "confirm_credential": f"cred-{ref}",
    }
    row.update(extra)
    return row


def _seed_versions():
    from app.services import segment_versioning as sv

    v1 = [
        _seg("B1", "T1", 1_000, 1_900, source_ids=[1, 2, 3]),
        _seg("B2", "T2", 2_000, 2_900, source_ids=[4, 5, 6]),
        _seg("B3", "T2", 3_000, 3_900, source_ids=[7, 8, 9]),
    ]
    sv.save_manifest("CASE-B", 1, v1, source_checksum="src-v1", status="已确认")

    v2 = [
        _seg("B1", "T1", 1_000, 1_900, source_ids=[1, 2, 3]),
        _seg(
            "B4", "T2", 2_000, 3_900, source_ids=[4, 5, 6, 7, 8, 9, 10],
            merged_from=["B2", "B3"], status="已审核", audit_status="已审核",
            confirmed=True, confirm_credential="cred-B4",
        ),
    ]
    sv.save_manifest("CASE-B", 2, v2, source_checksum="src-v2", status="已审核")


def test_versioned_handoff_recovery_relation_and_reverify(data_root):
    from app.services.track_segment import TrackSegmentService
    from app.services.route_restore import RouteRestoreService
    from app.services import segment_versioning as sv

    _seed_versions()
    track = TrackSegmentService()
    routes = RouteRestoreService()

    old = routes.start_restore("CASE-B", segment_version=1, task_id="old-task")
    assert [r["segment_ref"] for r in old["pending_routes"]] == ["B1", "B2", "B3"]

    first = routes.deliver_route("old-task")
    assert first["published"] is True
    assert first["delivered_route"]["segment_ref"] == "B1"
    interrupted = routes.interrupt_restore("old-task")
    assert interrupted["status"] == "interrupted"
    assert [r["segment_ref"] for r in interrupted["confirmed_routes"]] == ["B1"]

    audit = track.audit_confirmed_segment_replacements("CASE-B")
    relations = audit["relations"]
    assert len(relations) == 1
    rel = relations[0]
    assert rel["terminal"] == "T2"
    assert rel["original_segment_ids"] == ["B2", "B3"]
    assert rel["new_segment_id"] == "B4"

    record_only = track.get_segment_relations("CASE-B")
    assert record_only[0]["original_segment_ids"] == ["B2", "B3"]

    resumed = routes.resume_restore("old-task")
    assert [r["segment_ref"] for r in resumed["confirmed_routes"]] == ["B1"]
    assert [r["segment_ref"] for r in resumed["resumable_routes"]] == ["B2", "B3"]
    assert resumed["newly_delivered"] == []
    assert resumed["republished"] == []

    duplicate = routes.deliver_route("old-task", segment_ref="B1")
    assert duplicate["duplicate"] is True
    delivery_rows = sv.read_jsonl(resumed["delivery_file"])
    assert [r["segment_ref"] for r in delivery_rows] == ["B1"]

    continued = routes.resume_restore("old-task", deliver_all=True)
    assert [r["segment_ref"] for r in continued["newly_delivered"]] == ["B2", "B3"]
    assert continued["republished"] == []
    final_delivery_rows = sv.read_jsonl(resumed["delivery_file"])
    assert [r["segment_ref"] for r in final_delivery_rows] == ["B1", "B2", "B3"]

    new_task = routes.start_restore("CASE-B", segment_version=2, task_id="new-task")
    assert [r["segment_ref"] for r in new_task["pending_routes"]] == ["B1", "B4"]

    new_reverify = routes.reverify_terminal("CASE-B", "T2")
    assert new_reverify["segment_version"] == 2
    assert new_reverify["emitted_segment_ids"] == ["B4"]
    assert len(new_reverify["emitted_routes"]) == 1

    routes.open_restore_request("CASE-B", terminal="T2", segment_version=1, task_id="open-t2")
    old_reverify = routes.reverify_terminal("CASE-B", "T2", task_id="open-t2")
    assert old_reverify["segment_version"] == 1
    assert old_reverify["emitted_segment_ids"] == ["B2", "B3"]
    assert len(old_reverify["emitted_routes"]) == 2

    with pytest.raises(sv.MixedSegmentVersionError):
        sv.read_segment_version("CASE-B", segment_refs=["B2", "B3", "B4"])
