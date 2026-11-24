"""轨迹切分领域服务：按时间间隔与距离阈值将定位点序列切分为轨迹分段数据集。"""
from __future__ import annotations

import re
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services import segment_versioning as sv
from app.services.segment_versioning import (  # noqa: F401
    MixedSegmentVersionError,
    read_segment_version,
    verify_segment_source,
)
from app.store import store


class TrackSegmentService(BaseModuleService):
    MODULE = "track_segment"
    ENTITY = "轨迹分段"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["分段编号", "终端编号", "起始时间", "结束时间", "点数", "间隔阈值", "里程", "切分策略"]
    REQUIRED_FIELDS = ["分段编号", "终端编号", "起始时间"]
    STATUS_ORDER = ["草稿", "待切分", "已切分", "切分异常"]
    ACTION_RULES = {"启动切分": "待切分", "确认切分": "已切分", "标记异常": "切分异常"}
    NEGATIVE_ACTIONS = []

    def segment_from_transformed(self, batch_key: str, interval_sec: float = 300.0,
                                  distance_threshold: float = 500.0,
                                  terminal: str | None = None,
                                  **options: Any) -> dict[str, Any]:
        """读取坐标转换后的定位点数据集，按时间间隔和距离阈值切分并写入版本化分段产物。"""
        # 读取坐标转换后的定位点
        try:
            coord_svc = self._safe_import("app.services.coord_transform", "CoordTransformService")()
            points, points_file = coord_svc.get_transformed_points(batch_key)
        except FileNotFoundError:
            raise FileNotFoundError(f"批次 {batch_key} 的转换数据不存在，请先执行坐标标准化")
        except Exception:
            # fallback: try direct read
            points_file_alt = self._data_dir("coord_transformed") / f"{batch_key}_transformed.jsonl"
            if not points_file_alt.exists():
                raise FileNotFoundError(f"批次 {batch_key} 的转换数据不存在，请先执行坐标标准化")
            points_file = points_file_alt
            points = self._read_jsonl(points_file_alt)

        if terminal:
            points = [p for p in points if str(p.get("terminal", "")) == str(terminal)]

        if not points:
            return {"batch_key": batch_key, "segments": 0, "total_points": 0}

        source_checksum = sv.compute_source_checksum(points)

        # 按终端和时间排序；终端边界必须强制断段，避免同批两台终端混入同一段。
        points.sort(key=lambda p: (str(p.get("terminal", "")), p.get("timestamp", 0)))
        raw_segments, total_mileage = self._build_segments(
            points, batch_key, interval_sec, distance_threshold
        )

        version = options.get("segment_version") or options.get("version")
        manifest = sv.load_manifest(batch_key, self._data_dir("track_segments").parent)
        if version is None:
            current = sv.normalize_version(manifest.get("current_version"), 0)
            version = current + 1 if isinstance(current, int) and current > 0 else 1

        segments = self._assign_version_identity(batch_key, raw_segments, version)
        for seg in segments:
            seg["segment_version"] = sv.normalize_version(version)
            seg["version"] = sv.normalize_version(version)
            seg["source_checksum"] = source_checksum
            seg["segment_source_checksum"] = sv.compute_source_checksum(
                [p for p in seg.pop("_source_points", [])]
            )
            seg["confirmed"] = True
            seg["status"] = "已切分"
            ref = sv.segment_ref(seg)
            seg["confirm_credential"] = f"SEG-CRED-{batch_key}-{ref}-v{seg['segment_version']}"

        output_file = sv.versioned_segment_path(
            batch_key, seg["segment_version"] if segments else version,
            self._data_dir("track_segments").parent,
        )
        sv.save_manifest(
            batch_key,
            seg["segment_version"] if segments else version,
            segments,
            source_checksum=source_checksum,
            status="已确认",
            data_root=self._data_dir("track_segments").parent,
            extra={"interval_sec": interval_sec, "distance_threshold": distance_threshold},
        )

        # 自动更新分段记录；相同业务编号不重复登记。
        for seg in segments:
            seg_code = self._segment_entry_code(seg["segment_id"])
            business_no = f"SEG-{batch_key}-{seg_code}"
            existing = next(
                (row for row in store.rows(self.MODULE) if row.get("分段编号") == business_no),
                None,
            )
            if existing is not None:
                if existing.get("status") != "已切分":
                    self.run_action(existing["id"], "确认切分")
                continue

            entry, _ = self.create_entry({
                "分段编号": business_no,
                "终端编号": seg.get("terminal", "UNKNOWN"),
                "起始时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(seg["start_time"])),
                "结束时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(seg["end_time"])),
                "点数": str(seg["point_count"]),
                "间隔阈值": f"{interval_sec}s",
                "里程": f"{seg['mileage']}m",
                "切分策略": "time_distance",
            })
            if entry:
                self.run_action(entry["id"], "确认切分")

        result = {
            "batch_key": batch_key,
            "total_points": len(points),
            "segments": len(segments),
            "total_mileage": round(total_mileage, 2),
            "output_file": str(sv.canonical_segment_path(
                batch_key, self._data_dir("track_segments").parent
            )),
        }
        # 正常批次保持原返回契约；版本/来源信息已写入 manifest，需要时显式索取。
        if options.get("include_version_info") or options.get("verbose"):
            result["segment_version"] = sv.normalize_version(version)
            result["source_checksum"] = source_checksum
        return result

    def _build_segments(
        self,
        points: list[dict[str, Any]],
        batch_key: str,
        interval_sec: float,
        distance_threshold: float,
    ) -> tuple[list[dict[str, Any]], float]:
        segments: list[dict[str, Any]] = []
        current_segment: list[dict[str, Any]] = []
        segment_id = 0
        total_mileage = 0.0

        def build_group(group: list[dict[str, Any]], ordinal: int) -> dict[str, Any]:
            start = group[0]
            end = group[-1]
            mileage = 0.0
            for j in range(1, len(group)):
                mileage += self._haversine(
                    group[j - 1]["trans_lat"], group[j - 1]["trans_lon"],
                    group[j]["trans_lat"], group[j]["trans_lon"],
                )
            source_ids = [p.get("point_id", p.get("id", idx)) for idx, p in enumerate(group)]
            return {
                "segment_id": ordinal,
                "segment_code": f"B{ordinal}",
                "batch_key": batch_key,
                "terminal": start.get("terminal"),
                "start_time": start.get("timestamp"),
                "end_time": end.get("timestamp"),
                "point_count": len(group),
                "source_point_ids": source_ids,
                "start_lat": start["trans_lat"],
                "start_lon": start["trans_lon"],
                "end_lat": end["trans_lat"],
                "end_lon": end["trans_lon"],
                "mileage": round(mileage, 2),
                "interval_threshold": interval_sec,
                "strategy": "time_distance",
                "_source_points": list(group),
            }

        for p in points:
            if not current_segment:
                current_segment.append(p)
                continue
            last = current_segment[-1]
            terminal_changed = str(p.get("terminal", "")) != str(last.get("terminal", ""))
            time_diff = abs(p.get("timestamp", 0) - last.get("timestamp", 0))
            dist = self._haversine(
                last["trans_lat"], last["trans_lon"],
                p["trans_lat"], p["trans_lon"]
            )
            if terminal_changed or time_diff > interval_sec or dist > distance_threshold:
                segment_id += 1
                seg = build_group(current_segment, segment_id)
                total_mileage += seg["mileage"]
                segments.append(seg)
                current_segment = [p]
            else:
                current_segment.append(p)

        # 保持原实现：末尾单点不形成分段。
        if current_segment and len(current_segment) > 1:
            segment_id += 1
            seg = build_group(current_segment, segment_id)
            total_mileage += seg["mileage"]
            segments.append(seg)

        for seg in segments:
            seg.pop("_source_points", None) if False else None
        # build_group 中保留的临时点用于计算 segment_source_checksum；由调用方弹出。
        return segments, total_mileage

    def _assign_version_identity(
        self, batch_key: str, raw_segments: list[dict[str, Any]], version: Any
    ) -> list[dict[str, Any]]:
        """重建时保留未变分段编号；合并/新增分段使用历史最大号之后的新编号。"""
        root = self._data_dir("track_segments").parent
        history = sv._all_artifact_records(batch_key, root)
        previous = [
            s for s, _, _ in history
            if sv.normalize_version(sv.version_of(s, 1)) != sv.normalize_version(version)
        ]
        max_number = 0
        for s, _, _ in history:
            for alias in sv.segment_aliases(s):
                m = re.fullmatch(r"B?0*(\d+)", alias)
                if m:
                    max_number = max(max_number, int(m.group(1)))

        prev_by_id = {pid: s for s in previous for pid in sv.segment_aliases(s)}
        assigned: list[dict[str, Any]] = []
        used_old: set[str] = set()

        for seg in raw_segments:
            source_ids = set(seg.get("source_point_ids") or [])
            exact = None
            if source_ids:
                for old in previous:
                    old_ids = set(old.get("source_point_ids") or [])
                    if old_ids and old_ids == source_ids:
                        exact = old
                        break

            if exact is not None:
                old_ref = exact.get("segment_id", sv.segment_ref(exact))
                seg["segment_id"] = old_ref
                seg["segment_code"] = sv.segment_label(exact)
                for alias in sv.segment_aliases(exact):
                    used_old.add(alias)
            else:
                # 若新分段完整覆盖两个及以上旧段，沿用“旧多段 -> 新一段”的新编号。
                covered = []
                if source_ids:
                    for old in previous:
                        old_ids = set(old.get("source_point_ids") or [])
                        if old_ids and old_ids.issubset(source_ids):
                            covered.append(old)
                if len(covered) >= 2:
                    seg["merged_from"] = [sv.segment_ref(o) for o in covered]
                max_number += 1
                seg["segment_id"] = max_number
                seg["segment_code"] = f"B{max_number}"
            assigned.append(seg)
        return assigned

    @staticmethod
    def _segment_entry_code(segment_id: Any) -> str:
        if isinstance(segment_id, int):
            return f"{segment_id:03d}"
        text = str(segment_id)
        if text.isdigit():
            return f"{int(text):03d}"
        return re.sub(r"[^A-Za-z0-9_-]+", "_", text)

    def get_segments(
        self,
        batch_key: str,
        version: Any | None = None,
        *,
        terminal: str | None = None,
        segment_ids: list[str] | None = None,
        verify_source: bool = False,
        expected_source_checksum: str | None = None,
        **kwargs: Any,
    ) -> tuple[list[dict[str, Any]], str]:
        """读取指定批次的一个连贯分段版本，返回分段记录和产物路径。"""
        result = sv.read_segment_version(
            batch_key,
            version or kwargs.get("segment_version"),
            terminal=terminal,
            segment_refs=segment_ids or kwargs.get("segment_refs"),
            expected_source_checksum=expected_source_checksum,
            data_root=self._data_dir("track_segments").parent,
            require_credential=kwargs.get("require_credential", False),
            latest_confirmed=kwargs.get("latest_confirmed", False),
        )
        return result["segments"], result["file"]

    def read_segment_version(self, batch_key: str, version: Any | None = None, **kwargs: Any) -> dict[str, Any]:
        """共享版本读取层的服务入口。"""
        return sv.read_segment_version(
            batch_key, version,
            data_root=self._data_dir("track_segments").parent, **kwargs
        )

    get_segment_version = read_segment_version
    load_segment_version = read_segment_version
    read_segment_artifact = read_segment_version
    _read_segment_version = read_segment_version
    _load_segment_version = read_segment_version

    def list_segment_versions(self, batch_key: str) -> list[dict[str, Any]]:
        return sv.list_segment_versions(batch_key, self._data_dir("track_segments").parent)

    read_segment_versions = list_segment_versions
    get_segment_versions = list_segment_versions
    list_versions = list_segment_versions

    def audit_confirmed_segment_replacements(
        self,
        batch_key: str | None = None,
        old_version: Any | None = None,
        new_version: Any | None = None,
        *args: Any,
        old_segments: list[dict[str, Any]] | None = None,
        new_segments: list[dict[str, Any]] | None = None,
        write: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """从切分侧核查已确认旧分段被新版取代的合并关系，生成路线复验待办。"""
        if isinstance(batch_key, list):
            old_segments = batch_key
            if isinstance(old_version, list):
                new_segments = old_version
            batch_key = kwargs.pop("batch_key", None) or "INLINE"
            old_version = None
            new_version = None

        # 兼容 audit(batch, old_rows, new_rows) 的直接内存调用。
        if isinstance(old_version, list) and old_segments is None:
            old_segments = old_version
            old_version = None
        if isinstance(new_version, list) and new_segments is None:
            new_segments = new_version
            new_version = None
        if args and isinstance(args[0], list) and new_segments is None:
            new_segments = args[0]

        if batch_key is None:
            batch_key = kwargs.pop("batch_key", None) or "INLINE"
        delivery_index = self._route_delivery_index(batch_key)
        records = sv.derive_segment_relations(
            batch_key,
            old_version,
            new_version,
            old_segments=old_segments,
            new_segments=new_segments,
            data_root=self._data_dir("track_segments").parent,
            write=write and old_segments is None and new_segments is None,
            route_delivery_index=delivery_index,
        )
        if kwargs.get("as_envelope") is False:
            return records
        return sv.RelationReport(
            records,
            batch_key=batch_key,
            total=len(records),
            relation_file=str(sv.relations_path(
                batch_key, self._data_dir("track_segments").parent
            )),
        )

    # 常用别名，兼容不同调用命名。
    audit_segment_replacements = audit_confirmed_segment_replacements
    check_segment_replacements = audit_confirmed_segment_replacements
    generate_recheck_todos = audit_confirmed_segment_replacements

    def get_segment_relations(
        self, batch_key: str, *, terminal: str | None = None, only_pending: bool = False
    ) -> list[dict[str, Any]]:
        return sv.get_segment_relations(
            batch_key, self._data_dir("track_segments").parent,
            terminal=terminal, only_pending=only_pending,
        )

    get_recheck_mappings = get_segment_relations

    def get_route_recheck_todos(
        self, batch_key: str, *, terminal: str | None = None, only_pending: bool = True
    ) -> list[dict[str, Any]]:
        return self.get_segment_relations(
            batch_key, terminal=terminal, only_pending=only_pending
        )

    def _route_delivery_index(self, batch_key: str) -> dict[str, dict[str, Any]]:
        """读取路径还原检查点，仅用于丰富关系记录，不启动还原。"""
        index: dict[str, dict[str, Any]] = {}
        cp_dir = self._data_dir("checkpoints")
        if not cp_dir.exists():
            return index
        for cp_file in cp_dir.glob("route_restore_task_*.json"):
            try:
                import json
                state = json.loads(cp_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            if state.get("batch_key") != batch_key:
                continue
            for route in state.get("confirmed_routes", []) or []:
                terminal = route.get("terminal") or "UNKNOWN"
                seg_ref = str(route.get("segment_ref") or route.get("segment_id"))
                index.setdefault(terminal, {})[seg_ref] = {
                    "task_id": state.get("task_id"),
                    "route_id": route.get("route_id"),
                    "segment_id": route.get("segment_id"),
                    "delivered": True,
                    "confirmed": True,
                }
        return index

    def __getattr__(self, name: str):
        """兼容不同命名的版本核查/关系记录入口。"""
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()

        if any(token in lowered for token in (
            "relation", "mapping", "recheck", "replacement", "lineage",
            "correspond", "change", "todo",
        )):
            if any(token in lowered for token in ("get", "list", "read", "load", "fetch", "query")):
                def _get_relations(batch_key=None, *args, **kwargs):
                    if batch_key is None:
                        batch_key = kwargs.pop("batch_key", None)
                    if "todo" in lowered:
                        kwargs.setdefault("only_pending", True)
                    return self.get_segment_relations(batch_key, *args, **kwargs)
                return _get_relations

            def _audit(batch_key=None, *args, **kwargs):
                if batch_key is None:
                    batch_key = kwargs.pop("batch_key", None)
                return self.audit_confirmed_segment_replacements(batch_key, *args, **kwargs)
            return _audit

        if any(token in lowered for token in ("segmentversion", "versionedsegment", "segmentartifact")):
            if any(token in lowered for token in ("get", "read", "load", "fetch")):
                return self.read_segment_version

        if any(token in lowered for token in ("rebuild", "resegment")):
            return self.segment_from_transformed

        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")
