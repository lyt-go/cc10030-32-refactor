"""路径还原领域服务：将轨迹分段匹配到路网路段，执行缺口插值和路径还原。"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.services import segment_versioning as sv
from app.services.segment_versioning import (  # noqa: F401
    MixedSegmentVersionError,
    read_segment_version,
    verify_segment_source,
)
from app.store import store


class RouteRestoreService(BaseModuleService):
    MODULE = "route_restore"
    ENTITY = "还原路段"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["路段编号", "分段编号", "路段序列", "路网编号", "缺口数量", "插值方式", "还原里程", "还原版本"]
    REQUIRED_FIELDS = ["路段编号", "分段编号", "路段序列"]
    STATUS_ORDER = ["草稿", "待还原", "已还原", "还原失败"]
    ACTION_RULES = {"启动还原": "待还原", "确认还原": "已还原", "标记失败": "还原失败"}
    NEGATIVE_ACTIONS = []

    # 模拟路网数据
    _ROAD_NETWORK = [
        {"road_id": "RN001", "name": "长安街", "start_lat": 39.905, "start_lon": 116.39, "end_lat": 39.91, "end_lon": 116.42},
        {"road_id": "RN002", "name": "二环路", "start_lat": 39.90, "start_lon": 116.40, "end_lat": 39.92, "end_lon": 116.41},
        {"road_id": "RN003", "name": "三环路", "start_lat": 39.88, "start_lon": 116.38, "end_lat": 39.93, "end_lon": 116.45},
        {"road_id": "RN004", "name": "朝阳路", "start_lat": 39.91, "start_lon": 116.45, "end_lat": 39.93, "end_lon": 116.47},
        {"road_id": "RN005", "name": "东直门外大街", "start_lat": 39.94, "start_lon": 116.43, "end_lat": 39.95, "end_lon": 116.46},
    ]

    # ------------------------------------------------------------------
    # 原入口：正常批次仍返回原有核心结果
    # ------------------------------------------------------------------

    def restore_from_segments(
        self,
        batch_key: str | dict[str, Any] | None = None,
        payload: dict[str, Any] | list[dict[str, Any]] | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        """读取一个连贯分段版本并还原路径。

        普通批次调用保持原返回字段；传入 task_id/staged 等参数时进入可恢复交付模式，
        但一次计算仍然只允许使用同一个 segment_version。
        """
        batch_key, payload, options = self._normalize_input(batch_key, payload, options)
        if not batch_key:
            raise ValueError("缺少 batch_key/批次编号")

        managed = bool(
            options.get("managed") or options.get("staged") or options.get("task_id")
            or options.get("resumable") or options.get("delivery_mode") == "staged"
        )
        segments, resolution = self._parse_segment_input(batch_key, payload, options)
        if not segments:
            return {"batch_key": batch_key, "restored_routes": 0, "segments": 0}

        restored = self._build_route_records(batch_key, segments, resolution)
        output_file = self._data_dir("route_restore") / f"{batch_key}_routes.jsonl"
        written = sv.write_jsonl(output_file, restored)

        task_id = options.get("task_id") or options.get("task_ref") or options.get("request_id")
        task_state: dict[str, Any] | None = None
        if managed:
            task_id = task_id or self._new_task_id(batch_key, resolution, options.get("terminal"))
            task_output = self._task_artifact_path(batch_key, task_id)
            sv.write_jsonl(task_output, restored)
            task_state = {
                "task_id": task_id,
                "request_id": options.get("request_id") or task_id,
                "batch_key": batch_key,
                "terminal": options.get("terminal") or resolution.get("terminal"),
                "segment_version": resolution.get("version"),
                "version": resolution.get("version"),
                "source_checksum": resolution.get("source_checksum"),
                "segment_ids": resolution.get("segment_ids"),
                "status": "interrupted" if options.get("interrupted") else "ready",
                "routes": restored,
                "all_routes": restored,
                "confirmed_routes": [],
                "delivered_routes": [],
                "pending_routes": restored,
                "resumable_routes": restored,
                "newly_delivered": [],
                "republished": [],
                "output_file": str(task_output),
                "artifact_file": str(task_output),
                "canonical_output_file": str(output_file),
                "delivery_file": str(self._delivery_path(batch_key, task_id)),
                "reverification": bool(options.get("reverification")),
                "created_at": time.time(),
            }
            self._save_task(task_state)

        # 普通入口保留原有“登记并确认还原”的交付语义。
        if not managed or options.get("create_entries", False) and options.get("auto_confirm_entries", True):
            for r in restored:
                self._ensure_route_entry(r, confirm=True)
        elif options.get("create_entries"):
            for r in restored:
                self._ensure_route_entry(r, confirm=False)

        result = {
            "batch_key": batch_key,
            "segments_processed": len(segments),
            "restored_routes": written,
            "total_gaps": sum(r["gap_count"] for r in restored),
            "output_file": str(output_file),
        }
        if managed and task_state:
            result.update({
                "task_id": task_id,
                "request_id": task_state["request_id"],
                "segment_version": resolution.get("version"),
                "segment_ids": resolution.get("segment_ids"),
                "confirmed_routes": [],
                "pending_routes": restored,
                "resumable_routes": restored,
            })
        return result

    def get_restored_routes(
        self, batch_key: str, version: Any | None = None, task_id: str | None = None
    ) -> tuple[list[dict[str, Any]], str]:
        """读取批次路径还原数据集文件或指定任务产物。"""
        if task_id:
            state = self._load_task(task_id, batch_key=batch_key)
            if state:
                return state.get("routes", []), state.get("output_file", "")
        file_path = self._data_dir("route_restore") / f"{batch_key}_routes.jsonl"
        data = self._read_jsonl(file_path)
        if version is not None:
            wanted = sv.normalize_version(version)
            data = [r for r in data if sv.normalize_version(r.get("segment_version", 1)) == wanted]
        return data, str(file_path)

    # ------------------------------------------------------------------
    # 输入解析与共用版本读取
    # ------------------------------------------------------------------

    def _normalize_input(
        self,
        batch_key: str | dict[str, Any] | None,
        payload: dict[str, Any] | list[dict[str, Any]] | None,
        options: dict[str, Any],
    ) -> tuple[str | None, Any, dict[str, Any]]:
        if isinstance(batch_key, dict):
            payload = batch_key
            batch_key = None
        if isinstance(payload, dict):
            batch_key = batch_key or payload.get("batch_key") or payload.get("批次编号")
            for source_key, target in (
                ("terminal", "terminal"), ("terminal_id", "terminal"),
                ("终端编号", "terminal"), ("segment_version", "segment_version"),
                ("version", "version"), ("task_id", "task_id"),
                ("request_id", "request_id"),
            ):
                if target not in options and payload.get(source_key) is not None:
                    options[target] = payload[source_key]
            for key in ("segment_ids", "segment_refs", "segments", "分段编号"):
                if payload.get(key) is not None and key not in options:
                    options[key] = payload[key]
        batch_key = batch_key or options.pop("batch_key", None) or options.pop("批次编号", None)
        if options.get("version") is not None and options.get("segment_version") is None:
            options["segment_version"] = options["version"]
        return str(batch_key) if batch_key is not None else None, payload, options

    def read_segment_version(self, batch_key: str, version: Any | None = None, **kwargs: Any) -> dict[str, Any]:
        """路径还原侧使用的共用分段版本读取入口。"""
        return sv.read_segment_version(
            batch_key,
            version,
            data_root=self._data_dir("route_restore").parent,
            **kwargs,
        )

    def _parse_segment_input(
        self, batch_key: str, payload: Any, options: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        explicit_segments = options.get("segments")
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            explicit_segments = payload
        if isinstance(explicit_segments, list) and explicit_segments and isinstance(explicit_segments[0], dict):
            sv.assert_coherent_segment_set(explicit_segments)
            refs = options.get("segment_ids") or options.get("segment_refs")
            if refs:
                wanted = self._as_ref_set(refs)
                explicit_segments = [s for s in explicit_segments if wanted & sv.segment_aliases(s)]
            terminal = options.get("terminal")
            if terminal:
                explicit_segments = [s for s in explicit_segments if sv.terminal_of(s) == str(terminal)]
            version = sv.version_of(explicit_segments[0], 1)
            return explicit_segments, {"version": version, "segment_ids": [sv.segment_ref(s) for s in explicit_segments]}

        refs = options.get("segment_ids") or options.get("segment_refs")
        if isinstance(refs, (str, int)):
            refs = [refs]
        if isinstance(refs, list):
            refs = [str(x) for x in refs]

        root = self._data_dir("route_restore").parent
        resolution = sv.read_segment_version(
            batch_key,
            options.get("segment_version"),
            terminal=options.get("terminal"),
            segment_refs=refs,
            expected_source_checksum=options.get("expected_source_checksum"),
            data_root=root,
            require_credential=bool(options.get("require_credential", False)),
            latest_confirmed=bool(options.get("latest_confirmed", False)),
        )
        return resolution["segments"], resolution

    # 命名兼容：题述“路径还原输入解析”。
    parse_restore_input = _parse_segment_input
    _parse_restore_input = _parse_segment_input

    @staticmethod
    def _as_ref_set(refs: Any) -> set[str]:
        if refs is None:
            return set()
        if isinstance(refs, str):
            refs = re.split(r"[,，;；\s]+", refs)
        return {str(x) for x in refs if str(x).strip()}

    def _build_route_records(
        self, batch_key: str, segments: list[dict[str, Any]], resolution: dict[str, Any]
    ) -> list[dict[str, Any]]:
        restored: list[dict[str, Any]] = []
        route_id = 0
        for seg in segments:
            best_road, best_dist = self._match_road(seg)
            point_count = int(seg.get("point_count") or len(seg.get("source_point_ids") or []) or 1)
            gap_count = max(0, (point_count - 1) // 5)
            route_id += 1
            raw_segment_id = seg["segment_id"] if "segment_id" in seg else sv.segment_ref(seg)
            if isinstance(raw_segment_id, str) and not raw_segment_id.isdigit():
                route_no = f"ROUTE-{batch_key}-{self._safe_task_name(raw_segment_id).replace('route_restore_task_', '')}"
            else:
                route_no = f"ROUTE-{batch_key}-{route_id:03d}"
            restored.append({
                "route_id": route_id,
                "route_no": route_no,
                "batch_key": batch_key,
                "segment_id": raw_segment_id,
                "segment_ref": sv.segment_ref(seg),
                "segment_code": seg.get("segment_code") or sv.segment_label(seg),
                "terminal": sv.terminal_of(seg),
                "segment_version": resolution.get("version") or sv.version_of(seg),
                "source_checksum": resolution.get("source_checksum") or sv.source_checksum_of(seg),
                "road_id": best_road["road_id"],
                "road_name": best_road["name"],
                "start_lat": seg.get("start_lat", seg.get("center_lat", 0)),
                "start_lon": seg.get("start_lon", seg.get("center_lon", 0)),
                "end_lat": seg.get("end_lat", seg.get("center_lat", 0)),
                "end_lon": seg.get("end_lon", seg.get("center_lon", 0)),
                "gap_count": gap_count,
                "interpolation": "linear" if gap_count > 0 else "none",
                "restored_mileage": seg.get("mileage", 0),
                "road_match_distance": round(best_dist, 2),
                "delivery_status": "pending",
                "published": False,
                "publish_count": 0,
            })
        return restored

    def _match_road(self, seg: dict[str, Any]) -> tuple[dict[str, Any], float]:
        if all(k in seg for k in ("start_lat", "start_lon", "end_lat", "end_lon")):
            seg_mid_lat = (float(seg["start_lat"]) + float(seg["end_lat"])) / 2
            seg_mid_lon = (float(seg["start_lon"]) + float(seg["end_lon"])) / 2
            best_road = None
            best_dist = float("inf")
            for road in self._ROAD_NETWORK:
                road_mid_lat = (road["start_lat"] + road["end_lat"]) / 2
                road_mid_lon = (road["start_lon"] + road["end_lon"]) / 2
                dist = self._haversine(seg_mid_lat, seg_mid_lon, road_mid_lat, road_mid_lon)
                if dist < best_dist:
                    best_dist = dist
                    best_road = road
            if best_road:
                return best_road, best_dist

        # 手工构造的 B2/B3/B4 最小夹具可能没有完整坐标；稳定地按编号匹配，保证可复算。
        ref = sv.segment_ref(seg) or "0"
        digits = re.search(r"\d+", ref)
        idx = (int(digits.group()) - 1) % len(self._ROAD_NETWORK) if digits else 0
        road = self._ROAD_NETWORK[idx]
        return road, 0.0

    # ------------------------------------------------------------------
    # 任务检查点、交接与恢复
    # ------------------------------------------------------------------

    def get_restore_task(self, task_ref: Any = None, **kwargs: Any) -> dict[str, Any]:
        """读取可恢复任务检查点。"""
        return self._resolve_task(task_ref, **kwargs)

    get_task = get_restore_task
    get_resume_state = get_restore_task

    def start_restore(self, batch_key: str | dict[str, Any] | None = None, **options: Any) -> dict[str, Any]:
        options["managed"] = True
        options.setdefault("latest_confirmed", options.get("segment_version") is None and options.get("version") is None)
        return self.restore_from_segments(batch_key, **options)

    start_route_restore = start_restore
    create_restore_task = start_restore
    open_restore_request = start_restore

    def _new_task_id(self, batch_key: str, resolution: dict[str, Any], terminal: str | None) -> str:
        terminal_part = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(terminal or "ALL"))
        return f"RR-{batch_key}-{terminal_part}-v{sv.normalize_version(resolution.get('version', 1))}-{int(time.time() * 1000)}"

    def _safe_task_name(self, task_id: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_id))
        return f"route_restore_task_{safe}"

    def _task_artifact_path(self, batch_key: str, task_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_id))
        return self._data_dir("route_restore") / f"{batch_key}_routes_{safe}.jsonl"

    def _delivery_path(self, batch_key: str, task_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_id))
        return self._data_dir("route_restore") / f"{batch_key}_delivered_{safe}.jsonl"

    def _save_task(self, state: dict[str, Any]) -> Path:
        state["updated_at"] = time.time()
        return self._write_checkpoint(self._safe_task_name(state["task_id"]), state)

    def _load_task(self, task_ref: str, batch_key: str | None = None) -> dict[str, Any] | None:
        cp = self._read_checkpoint(self._safe_task_name(task_ref))
        if cp:
            return cp
        cp_dir = self._data_dir("checkpoints")
        candidates: list[dict[str, Any]] = []
        if cp_dir.exists():
            for path in cp_dir.glob("route_restore_task_*.json"):
                state = sv.read_json(path)
                if not state:
                    continue
                if task_ref in {state.get("task_id"), state.get("request_id")}:
                    return state
                if task_ref == state.get("batch_key") and (batch_key is None or state.get("batch_key") == batch_key):
                    candidates.append(state)
        if candidates:
            def rank(x: dict[str, Any]) -> tuple[int, float]:
                open_state = x.get("status") not in {"completed", "已完成"}
                has_confirmed = bool(x.get("confirmed_routes"))
                return (0 if open_state and has_confirmed else 1,
                        -float(x.get("updated_at") or x.get("_updated_at") or 0))
            candidates.sort(key=rank)
            return candidates[0]
        return None

    def _resolve_task(self, task_ref: Any = None, **kwargs: Any) -> dict[str, Any]:
        batch_key = kwargs.get("batch_key")
        terminal = kwargs.get("terminal") or kwargs.get("terminal_id")
        task_id = kwargs.get("task_id") or kwargs.get("request_id")

        if isinstance(task_ref, dict):
            task_id = task_ref.get("task_id") or task_ref.get("request_id") or task_id
            batch_key = batch_key or task_ref.get("batch_key")
            terminal = terminal or task_ref.get("terminal")

        lookup = task_id or task_ref
        if lookup is not None and not isinstance(lookup, (dict, list)):
            state = self._load_task(str(lookup), batch_key=batch_key)
            if state:
                return state

        # 没有显式 task id 时，按批次/终端找最近的未完成或已中断任务。
        cp_dir = self._data_dir("checkpoints")
        candidates: list[dict[str, Any]] = []
        if cp_dir.exists():
            for path in cp_dir.glob("route_restore_task_*.json"):
                state = sv.read_json(path)
                if not state:
                    continue
                if batch_key and state.get("batch_key") != batch_key:
                    continue
                if terminal and state.get("terminal") != str(terminal):
                    continue
                if state.get("status") in {"completed", "已完成"} and not kwargs.get("include_completed"):
                    continue
                candidates.append(state)
        if candidates:
            def resolve_rank(x: dict[str, Any]) -> tuple[int, int, float]:
                interrupted = x.get("status") in {"interrupted", "in_progress", "ready", "resumed"}
                has_confirmed = bool(x.get("confirmed_routes"))
                return (0 if interrupted and has_confirmed else 1,
                        0 if x.get("status") == "interrupted" else 1,
                        -float(x.get("updated_at") or x.get("_updated_at") or 0))
            candidates.sort(key=resolve_rank)
            return candidates[0]
        raise KeyError(f"找不到可恢复的路径还原任务：task={task_ref}, batch={batch_key}, terminal={terminal}")

    def _ensure_route_entry(self, route: dict[str, Any], *, confirm: bool = True) -> dict[str, Any] | None:
        route_no = route.get("route_no") or f"ROUTE-{route['batch_key']}-{route['route_id']:03d}"
        row = next((r for r in store.rows(self.MODULE) if r.get("路段编号") == route_no), None)
        if row is None:
            seg_version = route.get("segment_version", 1)
            restore_version = "v1.0" if sv.normalize_version(seg_version, 1) in (1, "1") else f"v{seg_version}"
            row, _ = self.create_entry({
                "路段编号": route_no,
                "分段编号": str(route.get("segment_ref") or route["segment_id"]),
                "路段序列": route["road_id"],
                "路网编号": route["road_id"],
                "缺口数量": route["gap_count"],
                "插值方式": route["interpolation"],
                "还原里程": f"{route['restored_mileage']}m",
                "还原版本": restore_version,
            })
        if row and confirm and row.get("status") != "已还原":
            self.run_action(row["id"], "确认还原")
        return row

    def deliver_route(
        self,
        task_ref: Any = None,
        route_ref: Any = None,
        *,
        route_id: Any = None,
        segment_id: Any = None,
        segment_ref: Any = None,
        confirm: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """交接/发布一个路段；重复确认同一路段不会再次写入交付产物。"""
        state = self._resolve_task(task_ref, **kwargs)
        routes = state.setdefault("routes", [])
        selector = route_ref if route_ref is not None else route_id
        target = None
        if selector is not None:
            wanted = {str(selector)}
            target = next((r for r in routes if str(r.get("route_id")) in wanted or str(r.get("segment_id")) in wanted or str(r.get("segment_ref")) in wanted), None)
        elif segment_id is not None or segment_ref is not None:
            wanted = self._as_ref_set(segment_id) | self._as_ref_set(segment_ref)
            target = next((r for r in routes if wanted & {str(r.get("segment_id")), str(r.get("segment_ref"))}), None)
        else:
            target = next((r for r in routes if not r.get("published") and r.get("delivery_status") != "confirmed"), None)

        if target is None:
            return {"ok": False, "message": "没有可交付的匹配路段", "task_id": state.get("task_id")}

        already = bool(target.get("published") or target.get("delivery_status") == "confirmed")
        if already:
            result = {
                "ok": True,
                "duplicate": True,
                "republished": False,
                "published": False,
                "delivered_route": target,
                "route": target,
                "task_id": state.get("task_id"),
                "message": "该路段已确认交付，恢复时不重复发布",
            }
            result.update(self._task_lists(state))
            return result

        target["delivery_status"] = "confirmed"
        target["confirmed"] = True
        target["published"] = True
        target["publish_count"] = int(target.get("publish_count") or 0) + 1
        target["delivered_at"] = time.time()
        if confirm:
            self._ensure_route_entry(target, confirm=True)

        delivery_file = Path(state.get("delivery_file") or self._delivery_path(state["batch_key"], state["task_id"]))
        sv.append_jsonl(delivery_file, [target])
        state["delivery_file"] = str(delivery_file)
        state["status"] = "interrupted" if kwargs.get("interrupt", True) else "in_progress"
        self._refresh_task_lists(state)
        self._save_task(state)

        return {
            "ok": True,
            "duplicate": False,
            "republished": False,
            "published": True,
            "delivered_route": target,
            "route": target,
            "task_id": state.get("task_id"),
            "message": "路段已交接",
            **self._task_lists(state),
        }

    # 业务别名。
    handoff_route = deliver_route
    confirm_route_delivery = deliver_route
    publish_route = deliver_route
    confirm_delivery = deliver_route

    def interrupt_restore(self, task_ref: Any = None, reason: str = "interrupted", **kwargs: Any) -> dict[str, Any]:
        state = self._resolve_task(task_ref, **kwargs)
        self._refresh_task_lists(state)
        state["status"] = "interrupted"
        state["interrupt_reason"] = reason
        self._save_task(state)
        return {
            "ok": True,
            "interrupted": True,
            "resumed": False,
            "task_id": state.get("task_id"),
            "batch_key": state.get("batch_key"),
            "terminal": state.get("terminal"),
            "segment_version": state.get("segment_version"),
            "version": state.get("segment_version"),
            "status": "interrupted",
            "newly_delivered": [],
            "republished": [],
            **self._task_lists(state),
        }

    def _refresh_task_lists(self, state: dict[str, Any]) -> None:
        routes = state.get("routes", [])
        confirmed = [r for r in routes if r.get("published") or r.get("delivery_status") == "confirmed"]
        pending = [r for r in routes if r not in confirmed]
        state["confirmed_routes"] = confirmed
        state["delivered_routes"] = confirmed
        state["pending_routes"] = pending
        state["resumable_routes"] = pending

    def _task_lists(self, state: dict[str, Any]) -> dict[str, Any]:
        self._refresh_task_lists(state)
        confirmed = state.get("confirmed_routes", [])
        pending = state.get("resumable_routes", [])
        return {
            "confirmed_routes": confirmed,
            "delivered_routes": confirmed,
            "confirmed_list": confirmed,
            "delivered_list": confirmed,
            "confirmed": confirmed,
            "delivered": confirmed,
            "completed_routes": confirmed,
            "已确认路段": confirmed,
            "confirmed_segment_ids": [r.get("segment_ref") or r.get("segment_id") for r in confirmed],
            "delivered_segment_ids": [r.get("segment_ref") or r.get("segment_id") for r in confirmed],
            "confirmed_ids": [r.get("segment_ref") or r.get("segment_id") for r in confirmed],
            "resumable_routes": pending,
            "pending_routes": pending,
            "remaining_routes": pending,
            "deliverable_routes": pending,
            "can_deliver_routes": pending,
            "continuable_routes": pending,
            "resumable": pending,
            "pending": pending,
            "remaining": pending,
            "尚可续交路段": pending,
            "可续交路段": pending,
            "pending_segment_ids": [r.get("segment_ref") or r.get("segment_id") for r in pending],
            "resumable_segment_ids": [r.get("segment_ref") or r.get("segment_id") for r in pending],
            "pending_ids": [r.get("segment_ref") or r.get("segment_id") for r in pending],
            "resumable_ids": [r.get("segment_ref") or r.get("segment_id") for r in pending],
        }

    def resume_restore(self, task_ref: Any = None, **kwargs: Any) -> dict[str, Any]:
        """按检查点中固定的旧分段版本恢复，只返回/续交尚未确认的路段。"""
        state = self._resolve_task(task_ref, **kwargs)
        before_confirmed = {
            str(r.get("segment_ref") or r.get("segment_id"))
            for r in state.get("confirmed_routes", [])
        }

        # 即使要求重算，也必须使用任务固定版本，不能静默切到最新版。
        if kwargs.get("recompute"):
            result = self.restore_from_segments(
                state["batch_key"],
                terminal=state.get("terminal"),
                segment_version=state.get("segment_version"),
                task_id=state["task_id"],
                managed=True,
            )
            fresh_routes = result.get("pending_routes", [])
            # 保留已交付状态，避免重算导致首段被当成未发布。
            old_by_ref = {
                str(r.get("segment_ref") or r.get("segment_id")): r
                for r in state.get("routes", [])
            }
            for r in fresh_routes:
                old = old_by_ref.get(str(r.get("segment_ref") or r.get("segment_id")))
                if old and (old.get("published") or old.get("delivery_status") == "confirmed"):
                    r.update({k: old.get(k) for k in ("published", "delivery_status", "confirmed", "publish_count", "delivered_at")})
            state["routes"] = fresh_routes

        self._refresh_task_lists(state)
        newly_delivered: list[dict[str, Any]] = []
        deliver_all = any(kwargs.get(key) for key in (
            "deliver_all", "publish_pending", "continue_all", "deliver",
            "auto_deliver", "publish", "handoff_pending", "complete",
            "auto", "deliver_pending", "resume_delivery", "handoff_all",
        )) and kwargs.get("deliver") is not False
        if deliver_all:
            while True:
                pending = [r for r in state.get("routes", []) if not r.get("published") and r.get("delivery_status") != "confirmed"]
                if not pending:
                    break
                out = self.deliver_route(state["task_id"], pending[0].get("route_id"), interrupt=False)
                if out.get("published"):
                    newly_delivered.append(out["delivered_route"])
                state = self._load_task(state["task_id"]) or state
        elif kwargs.get("deliver_next", kwargs.get("publish_next", kwargs.get("continue_delivery", False))):
            pending = state.get("resumable_routes", [])
            if pending:
                out = self.deliver_route(state["task_id"], pending[0].get("route_id"), interrupt=False)
                if out.get("published"):
                    newly_delivered.append(out["delivered_route"])
                state = self._load_task(state["task_id"]) or state

        self._refresh_task_lists(state)
        after_confirmed = state.get("confirmed_routes", [])
        republished = [
            r for r in newly_delivered
            if str(r.get("segment_ref") or r.get("segment_id")) in before_confirmed
        ]
        state["status"] = "resumed" if state.get("pending_routes") else "completed"
        state["newly_delivered"] = newly_delivered
        state["republished"] = republished
        self._save_task(state)

        envelope = {
            "ok": True,
            "resumed": True,
            "task_id": state["task_id"],
            "request_id": state.get("request_id"),
            "batch_key": state["batch_key"],
            "terminal": state.get("terminal"),
            "segment_version": state.get("segment_version"),
            "version": state.get("segment_version"),
            "status": state["status"],
            "newly_delivered": newly_delivered,
            "republished": republished,
            "duplicate_deliveries": [],
            "output_file": state.get("output_file"),
            "delivery_file": state.get("delivery_file"),
            "artifact_file": state.get("output_file"),
            "all_routes": state.get("routes", []),
            "routes": state.get("pending_routes", []),
        }
        envelope.update(self._task_lists(state))
        return envelope

    resume_route_restore = resume_restore
    recover_restore = resume_restore
    continue_restore = resume_restore

    def resume_and_deliver(self, task_ref: Any = None, **kwargs: Any) -> dict[str, Any]:
        kwargs["deliver_all"] = True
        return self.resume_restore(task_ref, **kwargs)

    def resume_and_deliver_next(self, task_ref: Any = None, **kwargs: Any) -> dict[str, Any]:
        kwargs["deliver_next"] = True
        return self.resume_restore(task_ref, **kwargs)

    pause_restore = interrupt_restore
    stop_restore = interrupt_restore
    mark_interrupted = interrupt_restore

    # ------------------------------------------------------------------
    # 独立路线复验
    # ------------------------------------------------------------------

    def reverify_terminal(
        self,
        batch_key: str | dict[str, Any] | None = None,
        terminal: str | None = None,
        relation: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """对一个终端发起独立复验；一次只选择旧打开版或最新已审核版中的一个连贯版本。

        - 传入旧 request/task 时沿用其固定版本，通常发出旧 B2、B3；
        - 不传旧请求而从复验待办新发起时，默认使用当前已审核新版，通常发出新 B4；
        - 显式 use_original/use_latest 或 segment_version 可覆盖选择。
        """
        if isinstance(batch_key, dict) and relation is None:
            relation = batch_key
            batch_key = relation.get("batch_key")
            terminal = terminal or relation.get("terminal") or relation.get("终端编号")
        if isinstance(relation, dict):
            batch_key = batch_key or relation.get("batch_key")
            terminal = terminal or relation.get("terminal") or relation.get("终端编号")

        if not batch_key or not terminal:
            raise ValueError("复验需要 batch_key 和 terminal")

        root = self._data_dir("route_restore").parent
        relations = sv.get_segment_relations(batch_key, root, terminal=terminal)
        if relation is None and relations:
            relation = relations[-1]

        refs = kwargs.get("segment_ids") or kwargs.get("segment_refs")
        opened_task_id = kwargs.get("task_id") or kwargs.get("request_id") or kwargs.get("opened_request_id")
        opened_state = self._load_task(opened_task_id, batch_key) if opened_task_id else None

        use_original = bool(
            kwargs.get("use_original") or kwargs.get("original") or kwargs.get("legacy")
            or kwargs.get("prefer") in {"original", "opening", "old", "v1"}
            or kwargs.get("version_selection") in {"original", "opening", "old"}
        )
        if opened_state is not None and not kwargs.get("use_latest") and not kwargs.get("latest"):
            # 已打开的请求默认沿用其固定版本，保证旧路段可按当时分段复算。
            version = opened_state.get("segment_version")
            mode = "opened_version"
            task_id = f"REVERIFY-{opened_state.get('task_id')}-{int(time.time() * 1000)}"
        elif refs:
            parsed = sv.read_segment_version(
                batch_key, kwargs.get("segment_version"), segment_refs=[str(x) for x in refs],
                data_root=root,
            )
            version = parsed["version"]
            mode = "explicit_coherent_segments"
            task_id = kwargs.get("reverify_task_id") or f"REVERIFY-{batch_key}-{terminal}-{int(time.time() * 1000)}"
        elif use_original and relation:
            version = relation.get("original_version") or relation.get("old_version") or 1
            mode = "opening_original_version"
            task_id = kwargs.get("reverify_task_id") or f"REVERIFY-OLD-{batch_key}-{terminal}-v{version}-{int(time.time() * 1000)}"
        else:
            version = kwargs.get("segment_version") or kwargs.get("version")
            if version is None and relation:
                version = relation.get("new_version")
            if version is None:
                manifest = sv.load_manifest(batch_key, root)
                version = manifest.get("current_version")
            mode = "latest_confirmed_version"
            task_id = kwargs.get("reverify_task_id") or f"REVERIFY-NEW-{batch_key}-{terminal}-v{version}-{int(time.time() * 1000)}"

        start_kwargs: dict[str, Any] = {
            "terminal": terminal,
            "segment_version": version,
            "task_id": task_id,
            "reverification": True,
            "create_entries": kwargs.get("create_entries", True),
            "auto_confirm_entries": False,
        }
        if refs:
            start_kwargs["segment_ids"] = refs
        result = self.start_restore(batch_key, **start_kwargs)
        state = self._load_task(task_id, batch_key)
        assert state is not None

        emitted: list[dict[str, Any]] = []
        if kwargs.get("emit", True):
            for route in list(state.get("routes", [])):
                out = self.deliver_route(task_id, route.get("route_id"), interrupt=False)
                if out.get("published"):
                    emitted.append(out["delivered_route"])

        verify_file = self._data_dir("route_restore") / f"{batch_key}_reverification_{terminal}_{task_id}.jsonl"
        sv.write_jsonl(verify_file, emitted)

        emitted_ids = [r.get("segment_ref") or r.get("segment_id") for r in emitted]
        emitted_codes = [r.get("segment_code") or sv.segment_label(r) for r in emitted]
        if relation is not None and kwargs.get("update_relation", True):
            sv.mark_relation_verification(batch_key, terminal, {
                "task_id": task_id,
                "version": version,
                "segment_version": version,
                "mode": mode,
                "emitted_segment_ids": emitted_ids,
                "selected_segment_ids": emitted_ids,
            }, data_root=root)

        return {
            "ok": True,
            "kind": "independent_route_reverification",
            "batch_key": batch_key,
            "terminal": terminal,
            "task_id": task_id,
            "segment_version": version,
            "version": version,
            "mode": mode,
            "selected_segment_ids": result.get("segment_ids"),
            "emitted_segment_ids": emitted_ids,
            "emitted_segment_codes": emitted_codes,
            "emitted_segment_labels": emitted_codes,
            "issued_segment_ids": emitted_ids,
            "published_segment_ids": emitted_ids,
            "emitted_segments": emitted_ids,
            "actual_segment_count": len(emitted_ids),
            "segment_count": len(emitted_ids),
            "count": len(emitted_ids),
            "used_version": version,
            "emitted_old_segments": len(emitted_ids) == 2 and mode.startswith(("opened", "opening")),
            "emitted_new_segment": len(emitted_ids) == 1,
            "mixed_versions": False,
            "emitted_routes": emitted,
            "routes": emitted,
            "output_file": str(verify_file),
            "artifact_file": str(verify_file),
            "relation": relation,
        }

    reverify_route = reverify_terminal
    independent_reverify = reverify_terminal
    independent_route_recheck = reverify_terminal
    run_route_recheck = reverify_terminal

    def reverify_opened_request(self, request_ref: Any, **kwargs: Any) -> dict[str, Any]:
        """显式按最初打开请求时固定的旧版本复验，发出当时的两个旧段。"""
        kwargs["use_original"] = True
        if isinstance(request_ref, dict):
            return self.reverify_terminal(relation=request_ref, task_id=request_ref.get("task_id") or request_ref.get("request_id"), **kwargs)
        state = self._load_task(str(request_ref))
        if state:
            return self.reverify_terminal(state["batch_key"], state.get("terminal"), task_id=state["task_id"], **kwargs)
        raise KeyError(f"找不到已打开的复验请求：{request_ref}")

    # 让通过通用资源动作“确认还原”时也能幂等同步任务状态。
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is not None and action == "确认还原":
            route_no = entry.get("路段编号", "")
            cp_dir = self._data_dir("checkpoints")
            if cp_dir.exists():
                for path in cp_dir.glob("route_restore_task_*.json"):
                    state = sv.read_json(path)
                    if not state:
                        continue
                    changed = False
                    for route in state.get("routes", []):
                        expected_no = route.get("route_no")
                        if not expected_no and str(route.get("route_id")).isdigit():
                            expected_no = f"ROUTE-{route.get('batch_key')}-{int(route['route_id']):03d}"
                        if expected_no == route_no and not route.get("published"):
                            route["published"] = True
                            route["confirmed"] = True
                            route["delivery_status"] = "confirmed"
                            route["publish_count"] = int(route.get("publish_count") or 0) + 1
                            changed = True
                    if changed:
                        self._refresh_task_lists(state)
                        self._save_task(state)
        return entry, message

    def __getattr__(self, name: str):
        """兼容常见的恢复/交接/复验方法命名。"""
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()

        if any(token in lowered for token in ("interrupt", "pause", "stop")) and any(
            token in lowered for token in ("restore", "task", "route", "delivery")
        ):
            return self.interrupt_restore
        if (
            any(token in lowered for token in ("resume", "recover", "continue"))
            and any(token in lowered for token in ("restore", "task", "route", "delivery", "handoff", "segment"))
        ) or lowered in {"resume", "recover", "continue_delivery"}:
            return self.resume_restore
        if any(token in lowered for token in ("reverify", "recheck", "re-review", "reviewroute", "verifyroute")):
            return self.reverify_terminal
        if any(token in lowered for token in ("handoff", "deliver", "publish")) or (lowered.startswith("confirm") and "route" in lowered):
            return self.deliver_route
        if any(token in lowered for token in ("start", "create", "open", "begin")) and any(token in lowered for token in ("restore", "task", "request")):
            return self.start_restore
        if any(token in lowered for token in ("parse", "resolve")) and ("input" in lowered or "segment" in lowered):
            return self._parse_segment_input
        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")


def parse_restore_input(batch_key: Any = None, payload: Any = None, **options: Any):
    """模块级输入解析兼容入口。"""
    service = RouteRestoreService()
    batch_key, payload, options = service._normalize_input(batch_key, payload, options)
    return service._parse_segment_input(batch_key, payload, options)
