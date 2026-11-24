"""分段产物的版本读取与来源核对共享层。

切分服务负责生产版本，路径还原/驻留等消费方只通过本模块读取同一个连贯版本，
避免把旧版本的 B2、B3 与新版本的 B4 混进同一次还原产物。
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Iterable

import app.services.base as base_mod

LEGACY_VERSION = "legacy"
CONFIRMED_STATUSES = {
    "已确认", "确认", "已切分", "已审核", "审核通过", "已生效", "已还原",
    "confirmed", "approved", "audited", "ready",
}
NEGATIVE_STATUSES = {"异常", "切分异常", "已驳回", "失败", "rejected", "failed"}
PREDECESSOR_KEYS = (
    "merged_from", "merge_from", "merged_source", "source_segments",
    "source_segment_refs", "replaces", "replaced_segment_ids",
    "original_segments", "old_segment_ids", "orig_segments",
    "原分段", "原分段编号", "合并来源", "被合并分段",
)


def predecessor_refs(segment: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    raw: Any = None
    for key in PREDECESSOR_KEYS:
        if segment.get(key) not in (None, "", []):
            raw = segment.get(key)
            break
    if isinstance(raw, list):
        for value in raw:
            refs.add(str(value))
    elif isinstance(raw, (str, int)):
        refs.update(x.strip() for x in re.split(r"[,，;；]", str(raw)) if x.strip())
    return refs


class SegmentVersionError(ValueError):
    """分段版本或来源不合法。"""


class MixedSegmentVersionError(SegmentVersionError):
    """一次输入中混入了多个分段版本。"""


class SegmentSourceMismatchError(SegmentVersionError):
    """分段来源凭据与请求的版本不一致。"""


class SegmentVersionNotFoundError(FileNotFoundError, SegmentVersionError):
    """找不到请求的分段版本。"""


class RelationReport(list):
    """关系核查结果：既可直接当记录列表，也可按信封字段读取。"""

    def __init__(self, records: list[dict[str, Any]] | None = None, **summary: Any) -> None:
        super().__init__(records or [])
        self.summary = summary

    @property
    def relations(self) -> list[dict[str, Any]]:
        return list(self)

    @property
    def items(self) -> list[dict[str, Any]]:
        return list(self)

    def __getitem__(self, key: Any):  # type: ignore[override]
        if isinstance(key, str):
            if key in {"relations", "items", "records", "data", "mappings", "correspondences"}:
                return list(self)
            if key == "total":
                return len(self)
            if key in self.summary:
                return self.summary[key]
            for record in self:
                if key in {
                    str(record.get("terminal", "")),
                    str(record.get("terminal_id", "")),
                    str(record.get("终端编号", "")),
                }:
                    return record
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> dict[str, Any]:
        return {**self.summary, "total": len(self), "relations": list(self), "items": list(self)}


# ---------------------------------------------------------------------------
# 路径与基础 IO
# ---------------------------------------------------------------------------

def _resolve_root(root: str | Path | None = None) -> Path:
    if root is not None:
        return Path(root)
    # 每次动态读取，兼容测试对 base_mod.DATA_ROOT 的 monkeypatch。
    return Path(base_mod.DATA_ROOT)


# Backwards/readable alias used internally before the resolver name.
data_root = _resolve_root


def segment_dir(root: str | Path | None = None) -> Path:
    path = _resolve_root(root) / "track_segments"
    path.mkdir(parents=True, exist_ok=True)
    return path


def canonical_segment_path(batch_key: str, root: str | Path | None = None) -> Path:
    return segment_dir(root) / f"{batch_key}_segments.jsonl"


def manifest_path(batch_key: str, root: str | Path | None = None) -> Path:
    return segment_dir(root) / f"{batch_key}_segment_versions.json"


def relations_path(batch_key: str, root: str | Path | None = None) -> Path:
    return segment_dir(root) / f"{batch_key}_segment_relations.jsonl"


def versioned_segment_path(
    batch_key: str, version: Any, root: str | Path | None = None
) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(version))
    return segment_dir(root) / f"{batch_key}_segments.{safe}.jsonl"


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def write_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> int:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    with p.open("w", encoding="utf-8") as f:
        for row in materialized:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    return len(materialized)


def append_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> int:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    with p.open("a", encoding="utf-8") as f:
        for row in materialized:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    return len(materialized)


def read_json(path: str | Path) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        value = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return value if isinstance(value, dict) else None


def write_json(path: str | Path, value: dict[str, Any]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# 字段归一化
# ---------------------------------------------------------------------------

def normalize_version(value: Any, default: Any = 1) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    text = str(value).strip()
    if text.lower() in {"latest", "current", "new", "newest"}:
        return text.lower()
    match = re.search(r"(\d+)", text)
    return int(match.group(1)) if match else text


def version_sort_key(version: Any) -> tuple[int, str]:
    v = normalize_version(version, LEGACY_VERSION)
    if isinstance(v, int):
        return (0, f"{v:012d}")
    if v == LEGACY_VERSION:
        return (-1, "")
    return (1, str(v))


def segment_ref(segment: dict[str, Any]) -> str | None:
    """返回分段的主引用，优先使用显式业务编号。"""
    for key in (
        "segment_code", "seg_code", "segment_no", "seg_no", "segment_ref",
        "segment_key", "分段编号",
    ):
        value = segment.get(key)
        if value is not None and str(value).strip():
            text = str(value).strip()
            # SEG-BATCH-B2 / SEG-BATCH-002 这类完整编号取尾号作为匹配别名之一，
            # 但主引用仍可能由 segment_id 提供。
            return text
    value = segment.get("segment_id", segment.get("id"))
    if value is None or str(value).strip() == "":
        return None
    return str(value)


def segment_aliases(segment: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    for key in (
        "segment_id", "id", "segment_code", "seg_code", "segment_no", "seg_no",
        "segment_ref", "segment_key", "分段编号", "编号",
        "old_segment_id", "new_segment_id",
    ):
        value = segment.get(key)
        if value is None or str(value).strip() == "":
            continue
        text = str(value).strip()
        aliases.add(text)
        if key == "分段编号" and "-" in text:
            tail = text.rsplit("-", 1)[-1]
            aliases.add(tail)
            aliases.add(tail.lstrip("0") or "0")
        if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
            aliases.add(f"B{value}")
            aliases.add(f"B{int(value):02d}")
            aliases.add(str(int(value)))
    ref = segment_ref(segment)
    if ref:
        aliases.add(ref)
        if ref.isdigit():
            aliases.add(f"B{ref}")
            aliases.add(f"B{int(ref):02d}")
    return {a for a in aliases if a}


def segment_label(segment: dict[str, Any]) -> str:
    ref = segment_ref(segment)
    if ref is None:
        return "UNKNOWN"
    if ref.isdigit():
        return f"B{int(ref)}"
    return ref


def terminal_of(segment: dict[str, Any]) -> str | None:
    for key in (
        "terminal", "terminal_id", "terminal_code", "device_id", "设备编号",
        "终端编号", "采集终端",
    ):
        value = segment.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def version_of(segment: dict[str, Any], default: Any = 1) -> Any:
    for key in (
        "segment_version", "seg_version", "version", "revision", "版本",
        "分段版本", "切分版本",
    ):
        if key in segment and segment.get(key) not in (None, ""):
            return normalize_version(segment.get(key), default)
    return default


def source_checksum_of(segment: dict[str, Any]) -> str | None:
    for key in (
        "source_checksum", "source_hash", "source_digest", "input_checksum",
        "artifact_checksum", "来源校验值", "来源核对值", "批次校验值",
    ):
        value = segment.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def has_confirmable_credential(segment: dict[str, Any], manifest_entry: dict[str, Any] | None = None) -> bool:
    """旧版/新版分段只要保留状态、布尔确认或凭据之一即可复核。"""
    if manifest_entry:
        if manifest_entry.get("confirmed") is True:
            return True
        status = str(manifest_entry.get("status", ""))
        if status in CONFIRMED_STATUSES:
            return True
        if manifest_entry.get("credential") or manifest_entry.get("confirm_credential"):
            return True

    if segment.get("confirmed") is True or segment.get("is_confirmed") is True:
        return True
    status = str(segment.get("status", ""))
    if status in NEGATIVE_STATUSES or segment.get("abnormal") is True:
        return False
    if status in CONFIRMED_STATUSES:
        return True
    for key in (
        "credential", "confirm_credential", "confirmation", "confirm_token",
        "audit_credential", "审核凭据", "确认凭据", "审核信息",
    ):
        if segment.get(key):
            return True
    if segment.get("audit_status") in CONFIRMED_STATUSES:
        return True
    # 没有明确否定状态的历史产物也允许按当时版本复算；是否必须显式凭据由调用方决定。
    return status not in NEGATIVE_STATUSES


def compute_source_checksum(points: list[dict[str, Any]] | None = None, *, raw: str | bytes | None = None) -> str:
    import hashlib

    if raw is not None:
        content = raw if isinstance(raw, bytes) else str(raw).encode("utf-8")
    else:
        stable = []
        for p in points or []:
            stable.append({
                "point_id": p.get("point_id") or p.get("id"),
                "terminal": p.get("terminal"),
                "timestamp": p.get("timestamp"),
                "lat": p.get("trans_lat", p.get("lat")),
                "lon": p.get("trans_lon", p.get("lon")),
            })
        content = json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(content).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Manifest 与版本列举
# ---------------------------------------------------------------------------

def load_manifest(batch_key: str, data_root: str | Path | None = None) -> dict[str, Any]:
    path = manifest_path(batch_key, data_root)
    manifest = read_json(path) or {
        "batch_key": batch_key,
        "current_version": None,
        "versions": {},
    }
    manifest.setdefault("batch_key", batch_key)
    manifest.setdefault("versions", {})
    return manifest


def save_manifest(
    batch_key: str,
    version: Any,
    segments: list[dict[str, Any]],
    *,
    source_checksum: str | None = None,
    status: str = "已确认",
    data_root: str | Path | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = _resolve_root(data_root)
    manifest = load_manifest(batch_key, root)
    version_no = normalize_version(version, 1)
    file_path = versioned_segment_path(batch_key, version_no, root)
    write_jsonl(file_path, segments)
    write_jsonl(canonical_segment_path(batch_key, root), segments)

    refs = [r for r in (segment_ref(s) for s in segments) if r is not None]
    entry = {
        "version": version_no,
        "file": str(file_path),
        "canonical_file": str(canonical_segment_path(batch_key, root)),
        "segment_ids": refs,
        "segment_count": len(segments),
        "source_checksum": source_checksum,
        "status": status,
        "confirmed": status in CONFIRMED_STATUSES,
        "created_at": time.time(),
    }
    if extra:
        entry.update(extra)
    manifest["versions"][str(version_no)] = entry
    if isinstance(version_no, int):
        current = normalize_version(manifest.get("current_version"), 0)
        if not isinstance(current, int) or version_no >= current:
            manifest["current_version"] = version_no
    else:
        manifest["current_version"] = version_no
    manifest["updated_at"] = time.time()
    write_json(manifest_path(batch_key, root), manifest)
    return manifest


def _version_from_path(path: Path) -> Any | None:
    match = re.search(r"segments[._-]v?([A-Za-z0-9_-]+)\.jsonl$", path.name)
    if not match:
        return None
    value = match.group(1)
    if value.lower() == "jsonl":
        return None
    return normalize_version(value, None)


def _all_artifact_records(
    batch_key: str, data_root: str | Path | None = None
) -> list[tuple[dict[str, Any], Path, Any]]:
    root = _resolve_root(data_root)
    raw: list[tuple[dict[str, Any], Path, Any]] = []
    canonical = canonical_segment_path(batch_key, root)

    pattern = f"{batch_key}_segments*.jsonl"
    for path in sorted(segment_dir(root).glob(pattern)):
        hint = _version_from_path(path)
        for row in read_jsonl(path):
            raw.append((row, path, hint))

    manifest = load_manifest(batch_key, root)
    for entry in manifest.get("versions", {}).values():
        file_name = entry.get("file")
        entry_version = normalize_version(entry.get("version"), None)
        if file_name and Path(file_name).exists():
            p = Path(file_name)
            known_paths = {str(path) for _, path, _ in raw}
            if str(p) not in known_paths:
                for row in read_jsonl(p):
                    raw.append((row, p, entry_version))

    # 去重：canonical 与 vN 快照可能保存同一版本；按版本+分段引用只保留一条。
    deduped: dict[tuple[str, str], tuple[dict[str, Any], Path, Any]] = {}
    for row, path, hint in raw:
        version = version_of(row, hint if hint is not None else manifest.get("current_version") or 1)
        ref = segment_ref(row) or json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        key = (str(version), str(ref))
        # 优先保留带明确版本提示的快照路径。
        if key not in deduped or (hint is not None and deduped[key][2] is None):
            deduped[key] = (row, path, hint if hint is not None else version)

    records = list(deduped.values())
    if not records and canonical.exists():
        for row in read_jsonl(canonical):
            records.append((row, canonical, version_of(row, 1)))
    return records


def list_segment_versions(
    batch_key: str, data_root: str | Path | None = None
) -> list[dict[str, Any]]:
    manifest = load_manifest(batch_key, data_root)
    result: dict[Any, dict[str, Any]] = {}

    for key, entry in manifest.get("versions", {}).items():
        v = normalize_version(entry.get("version", key), key)
        result[v] = dict(entry)
        result[v]["version"] = v

    canonical = canonical_segment_path(batch_key, data_root)
    groups: dict[Any, list[dict[str, Any]]] = {}
    for seg in read_jsonl(canonical):
        groups.setdefault(version_of(seg), []).append(seg)
    for v, segs in groups.items():
        result.setdefault(v, {
            "version": v,
            "file": str(canonical),
            "segment_ids": [segment_ref(s) for s in segs],
            "segment_count": len(segs),
            "source_checksum": next((source_checksum_of(s) for s in segs if source_checksum_of(s)), None),
            "status": next((str(s.get("status")) for s in segs if s.get("status")), None),
        })
    return [result[v] for v in sorted(result, key=version_sort_key)]


# ---------------------------------------------------------------------------
# 共用读取入口
# ---------------------------------------------------------------------------

def assert_coherent_segment_set(segments: list[dict[str, Any]]) -> None:
    """校验一组分段既不跨显式版本，也不同时包含合并前驱与后继。"""
    versions = {version_of(s) for s in segments}
    if len(versions) > 1:
        raise MixedSegmentVersionError(f"分段包含多个版本：{sorted(versions, key=version_sort_key)}")
    selected_aliases = set().union(*(segment_aliases(s) for s in segments)) if segments else set()
    for seg in segments:
        mixed = predecessor_refs(seg) & selected_aliases
        own = segment_aliases(seg)
        if mixed and not (mixed <= own):
            raise MixedSegmentVersionError(
                f"新版分段 {segment_ref(seg)} 与旧段 {sorted(mixed)} 不能出现在同一次还原/复验中"
            )


def _drop_superseded_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """无显式版本号的追加产物中，移除已被同文件新段显式取代的旧前驱段。"""
    superseded: set[str] = set()
    all_aliases = set().union(*(segment_aliases(s) for s in segments)) if segments else set()
    for seg in segments:
        for ref in predecessor_refs(seg):
            # 只有后继段也在当前产物中时才淘汰，避免误删仅有引用文本的历史快照。
            if ref in all_aliases:
                superseded.add(ref)
    if not superseded:
        return segments
    return [s for s in segments if not (segment_aliases(s) & superseded)]


def read_segment_version(
    batch_key: str,
    version: Any | None = None,
    *,
    terminal: str | None = None,
    segment_refs: Iterable[str] | None = None,
    expected_source_checksum: str | None = None,
    data_root: str | Path | None = None,
    require_credential: bool = False,
    latest_confirmed: bool = False,
) -> dict[str, Any]:
    """读取一个连贯分段版本。

    - 指定 segment_refs 时，这些引用必须全部来自同一版本/来源；
    - 不指定 version 时读取 manifest 当前版或产物中的最高连贯版本；
    - 永远不会把不同 version 的记录拼进同一个返回 segments。
    """
    root = _resolve_root(data_root)
    manifest = load_manifest(batch_key, root)
    using_default_version = version is None
    requested_refs = [str(x).strip() for x in (segment_refs or []) if str(x).strip()]
    all_records = _all_artifact_records(batch_key, root)
    if isinstance(version, str) and version.strip().lower() in {"old", "original", "opening", "previous", "prev"}:
        known_versions = list_segment_versions(batch_key, root)
        requested_version = known_versions[-2]["version"] if len(known_versions) >= 2 else 1
    elif isinstance(version, str) and version.strip().lower() in {"new", "newest", "latest", "current"}:
        current = manifest.get("current_version")
        requested_version = normalize_version(current, None) if current else max(
            {version_of(row, hint if hint is not None else 1) for row, _, hint in all_records},
            key=version_sort_key,
        )
    else:
        requested_version = normalize_version(version, None) if version is not None else None

    if not all_records:
        raise SegmentVersionNotFoundError(f"批次 {batch_key} 的轨迹分段数据不存在")

    # 显式分段引用：先解析版本，并执行最关键的跨版本混合校验。
    if requested_refs:
        wanted = set(requested_refs)
        matched: list[dict[str, Any]] = []
        matched_paths: set[str] = set()
        versions: set[Any] = set()
        for seg, path, hint in all_records:
            aliases = segment_aliases(seg)
            if wanted & aliases:
                matched.append(seg)
                matched_paths.add(str(path))
                fallback = hint if hint is not None else (manifest.get("current_version") or 1)
                versions.add(version_of(seg, fallback))

        found_aliases = set().union(*(segment_aliases(s) for s in matched)) if matched else set()
        missing = [r for r in requested_refs if r not in found_aliases]
        if missing:
            raise SegmentVersionNotFoundError(
                f"批次 {batch_key} 找不到分段引用：{', '.join(missing)}"
            )
        if len(versions) > 1:
            raise MixedSegmentVersionError(
                f"分段引用 {requested_refs} 跨了多个版本 {sorted(versions, key=version_sort_key)}，"
                "不能在同一次路径还原/复验中混合"
            )

        # 即使夹具没有显式 version 字段，只要同时选择了 merged_from/replaces
        # 的前驱和后继（例如 B2、B3 与 B4），也属于把两版拼进同一次产物。
        selected_alias_set = set().union(*(segment_aliases(s) for s in matched))
        for seg in matched:
            mixed_predecessors = predecessor_refs(seg) & selected_alias_set
            own_aliases = segment_aliases(seg)
            if mixed_predecessors and not (mixed_predecessors <= own_aliases):
                raise MixedSegmentVersionError(
                    f"新版分段 {segment_ref(seg)} 取代了 {sorted(mixed_predecessors)}，"
                    "不能把旧段和新段拼在同一次路径还原/复验中"
                )

        resolved_version = requested_version or next(iter(versions), 1)
        if requested_version is not None and normalize_version(resolved_version) != requested_version:
            raise MixedSegmentVersionError(
                f"请求版本 {requested_version} 与分段引用实际版本 {resolved_version} 不一致"
            )
        segments = matched
        file_path = next(iter(matched_paths)) if len(matched_paths) == 1 else str(canonical_segment_path(batch_key, root))
    else:
        versions = {
            version_of(seg, hint if hint is not None else manifest.get("current_version") or 1)
            for seg, _, hint in all_records
        }
        if requested_version is None:
            if latest_confirmed:
                candidates = list_segment_versions(batch_key, root)
                confirmed = [
                    v for v in candidates
                    if has_confirmable_credential({}, manifest.get("versions", {}).get(str(v["version"])))
                ]
                if confirmed:
                    requested_version = confirmed[-1]["version"]
            if requested_version is None:
                current = manifest.get("current_version")
                requested_version = normalize_version(current, None) if current else max(versions, key=version_sort_key)

        manifest_entry = manifest.get("versions", {}).get(str(requested_version))
        path: Path | None = None
        segments: list[dict[str, Any]] = []

        if manifest_entry and manifest_entry.get("file"):
            candidate = Path(manifest_entry["file"])
            if candidate.exists():
                path = candidate
                segments = read_jsonl(candidate)

        if not segments:
            candidate = versioned_segment_path(batch_key, requested_version, root)
            if candidate.exists():
                path = candidate
                segments = read_jsonl(candidate)

        if not segments:
            canonical = canonical_segment_path(batch_key, root)
            canonical_rows = read_jsonl(canonical)
            grouped = [s for s in canonical_rows if version_of(s) == requested_version]
            if grouped:
                path = canonical
                segments = grouped

        if not segments:
            # 兼容 _v1 / .v1 / 自定义快照命名：从扫描到的全部产物中按版本提示归组。
            grouped_records = [
                (seg, path) for seg, path, hint in all_records
                if normalize_version(
                    version_of(seg, hint if hint is not None else requested_version)
                ) == normalize_version(requested_version)
            ]
            if grouped_records:
                segments = [seg for seg, _ in grouped_records]
                path = grouped_records[0][1]

        if not segments:
            # 兼容只给了 canonical 旧文件、没有版本字段的情况。
            canonical = canonical_segment_path(batch_key, root)
            canonical_rows = read_jsonl(canonical)
            if canonical_rows and requested_version in (1, LEGACY_VERSION):
                path = canonical
                segments = canonical_rows

        if not segments:
            raise SegmentVersionNotFoundError(
                f"批次 {batch_key} 的分段版本 {requested_version} 不存在"
            )
        if using_default_version:
            segments = _drop_superseded_segments(segments)
        file_path = str(path or canonical_segment_path(batch_key, root))
        resolved_version = requested_version

    if terminal:
        segments = [s for s in segments if terminal_of(s) == str(terminal)]

    # 二次一致性校验：过滤后仍必须是同一版本。
    segment_versions = {version_of(s, resolved_version) for s in segments}
    if len(segment_versions) > 1:
        raise MixedSegmentVersionError(
            f"批次 {batch_key} 读取到混合分段版本：{sorted(segment_versions, key=version_sort_key)}"
        )

    manifest_entry = manifest.get("versions", {}).get(str(resolved_version))
    source_checksum = (
        (manifest_entry or {}).get("source_checksum")
        or next((source_checksum_of(s) for s in segments if source_checksum_of(s)), None)
    )
    if expected_source_checksum and source_checksum and expected_source_checksum != source_checksum:
        raise SegmentSourceMismatchError(
            f"分段来源核对失败：期望 {expected_source_checksum}，实际 {source_checksum}"
        )

    if require_credential:
        valid = [
            s for s in segments
            if has_confirmable_credential(s, manifest_entry)
        ]
        if len(valid) != len(segments):
            raise SegmentSourceMismatchError("部分分段缺少可确认凭据，不能按已确认版本交付")

    return {
        "batch_key": batch_key,
        "version": resolved_version,
        "segment_version": resolved_version,
        "segments": segments,
        "file": file_path,
        "source_checksum": source_checksum,
        "manifest_entry": manifest_entry,
        "terminal": terminal,
        "segment_ids": [segment_ref(s) for s in segments],
    }


# 常见命名别名：不同调用方可能把“读取版本”叫成 load/get。
load_segment_version = read_segment_version
get_segment_version = read_segment_version
read_segment_artifact = read_segment_version
load_segment_artifact = read_segment_version


# ---------------------------------------------------------------------------
# 新旧版本对应关系
# ---------------------------------------------------------------------------

def _source_ids(seg: dict[str, Any]) -> set[Any]:
    for key in ("source_point_ids", "point_ids", "source_ids", "point_id_range"):
        value = seg.get(key)
        if isinstance(value, list):
            return set(value)
        if isinstance(value, (tuple, set)):
            return set(value)
    start = seg.get("source_start_point_id") or seg.get("start_point_id")
    end = seg.get("source_end_point_id") or seg.get("end_point_id")
    if start is not None and end is not None:
        try:
            return set(range(int(start), int(end) + 1))
        except (TypeError, ValueError):
            return {start, end}
    return set()


def _time_range(seg: dict[str, Any]) -> tuple[float, float] | None:
    start = seg.get("start_time") or seg.get("start_timestamp")
    end = seg.get("end_time") or seg.get("end_timestamp")
    if start is None or end is None:
        return None
    try:
        return (float(start), float(end))
    except (TypeError, ValueError):
        return None


def _coverage(old: dict[str, Any], new: dict[str, Any]) -> bool:
    old_ids = _source_ids(old)
    new_ids = _source_ids(new)
    if old_ids and new_ids:
        return len(old_ids & new_ids) > 0
    old_t = _time_range(old)
    new_t = _time_range(new)
    if old_t and new_t:
        return not (old_t[1] < new_t[0] or new_t[1] < old_t[0])
    # 最小夹具可能只有终端和编号；同一终端的旧多段到新单段按顺序归并。
    return terminal_of(old) == terminal_of(new)


def relation_file(batch_key: str, data_root: str | Path | None = None) -> Path:
    return relations_path(batch_key, data_root)


def get_segment_relations(
    batch_key: str,
    data_root: str | Path | None = None,
    *,
    terminal: str | None = None,
    only_pending: bool = False,
) -> list[dict[str, Any]]:
    path = relations_path(batch_key, data_root)
    rows = read_jsonl(path)
    if not rows and path.with_suffix(".json").exists():
        payload = read_json(path.with_suffix(".json")) or {}
        rows = payload.get("relations") or payload.get("items") or []
    if terminal is not None:
        rows = [r for r in rows if str(r.get("terminal") or r.get("终端编号")) == str(terminal)]
    if only_pending:
        rows = [r for r in rows if r.get("status") in {"待复验", "pending", "open"} or r.get("todo_status") == "pending"]
    return rows


def load_relation_record(path_or_batch: str | Path, data_root: str | Path | None = None) -> list[dict[str, Any]]:
    p = Path(path_or_batch)
    if p.exists():
        return read_jsonl(p)
    return get_segment_relations(str(path_or_batch), data_root)


def derive_segment_relations(
    batch_key: str,
    old_version: Any | None = None,
    new_version: Any | None = None,
    *,
    old_segments: list[dict[str, Any]] | None = None,
    new_segments: list[dict[str, Any]] | None = None,
    data_root: str | Path | None = None,
    write: bool = True,
    route_delivery_index: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """核查旧确认分段被新版取代的合并/替换关系。未改动终端不会产生记录。"""
    root = _resolve_root(data_root)
    versions = list_segment_versions(batch_key, root)

    if old_segments is None or new_segments is None:
        if len(versions) < 2 and (old_version is None or new_version is None):
            return []
        if old_version is None:
            old_version = versions[-2]["version"] if len(versions) >= 2 else 1
        if new_version is None:
            new_version = versions[-1]["version"]
        old = read_segment_version(batch_key, old_version, data_root=root) if old_segments is None else {"segments": old_segments, "version": old_version}
        new = read_segment_version(batch_key, new_version, data_root=root) if new_segments is None else {"segments": new_segments, "version": new_version}
        old_segments = old["segments"]
        new_segments = new["segments"]
        old_version = old["version"]
        new_version = new["version"]

    assert old_segments is not None and new_segments is not None
    old_by_terminal: dict[str, list[dict[str, Any]]] = {}
    new_by_terminal: dict[str, list[dict[str, Any]]] = {}
    for seg in old_segments:
        old_by_terminal.setdefault(terminal_of(seg) or "UNKNOWN", []).append(seg)
    for seg in new_segments:
        new_by_terminal.setdefault(terminal_of(seg) or "UNKNOWN", []).append(seg)

    records: list[dict[str, Any]] = []
    for terminal in sorted(set(old_by_terminal) | set(new_by_terminal)):
        olds = old_by_terminal.get(terminal, [])
        news = new_by_terminal.get(terminal, [])
        old_refs = [segment_ref(s) for s in olds if segment_ref(s)]
        new_refs = [segment_ref(s) for s in news if segment_ref(s)]
        old_set = set(old_refs)
        new_set = set(new_refs)

        # 编号和段数完全一致视为未改动，绝不登记。
        if len(olds) == len(news) and old_set == new_set:
            continue

        mappings: list[tuple[list[dict[str, Any]], dict[str, Any], str]] = []

        # 优先采用新分段中显式声明的 merged_from/replaces。
        for new_seg in news:
            explicit_refs = predecessor_refs(new_seg)
            explicit_olds = [s for s in olds if explicit_refs & segment_aliases(s)]
            if len(explicit_olds) >= 2:
                mappings.append((explicit_olds, new_seg, "explicit_merge"))

        # 来源点/时间范围重叠推断。
        if not mappings and len(olds) >= 2 and len(news) == 1:
            new_seg = news[0]
            covered = [s for s in olds if _coverage(s, new_seg)]
            if len(covered) == len(olds):
                mappings.append((olds, new_seg, "source_overlap_merge"))

        for old_group, new_seg, reason in mappings:
            original_ids = [segment_ref(s) for s in old_group if segment_ref(s)]
            original_labels = [segment_label(s) for s in old_group]
            new_id = segment_ref(new_seg)
            new_label = segment_label(new_seg)
            if not new_id or set(original_ids) == {new_id}:
                continue
            if not all(has_confirmable_credential(s) for s in old_group):
                continue

            delivery = (route_delivery_index or {}).get(terminal, {})
            affected_routes = []
            delivered = False
            for old_id in original_ids:
                info = delivery.get(old_id) if isinstance(delivery, dict) else None
                if info:
                    affected_routes.append(info)
                    delivered = delivered or bool(info.get("delivered") or info.get("confirmed"))

            record = {
                "relation_id": f"REL-{batch_key}-{terminal}-v{normalize_version(old_version)}-v{normalize_version(new_version)}",
                "batch_key": batch_key,
                "terminal": terminal,
                "terminal_id": terminal,
                "终端编号": terminal,
                "change_type": "merged",
                "relation_type": "merge",
                "关系类型": "合并",
                "reason": reason,
                "original_version": normalize_version(old_version),
                "old_version": normalize_version(old_version),
                "source_version": normalize_version(old_version),
                "from_version": normalize_version(old_version),
                "new_version": normalize_version(new_version),
                "target_version": normalize_version(new_version),
                "to_version": normalize_version(new_version),
                "original_segment_ids": original_ids,
                "original_segment_refs": original_ids,
                "original_segment_labels": original_labels,
                "old_segment_ids": original_ids,
                "source_segment_ids": original_ids,
                "from_segment_ids": original_ids,
                "before_segment_ids": original_ids,
                "原分段编号": original_ids,
                "new_segment_id": new_id,
                "new_segment_label": new_label,
                "merged_segment_id": new_id,
                "replacement_segment_id": new_id,
                "to_segment_id": new_id,
                "after_segment_id": new_id,
                "target_segment_id": new_id,
                "merged_to": new_id,
                "replaced_by": new_id,
                "新分段编号": new_id,
                "segment_mapping": {"old": original_ids, "new": new_id},
                "affected_route_refs": affected_routes,
                "has_delivered_route": delivered,
                "recheck_options": [
                    {"version": normalize_version(old_version), "segment_ids": original_ids, "mode": "opening_version"},
                    {"version": normalize_version(new_version), "segment_ids": [new_id], "mode": "replacement_version"},
                ],
                "forbidden_mixed_segment_ids": original_ids + [new_id],
                "must_not_combine_versions": True,
                "recheck_policy": "choose_one_coherent_version",
                "status": "待复验",
                "todo_status": "pending",
                "复验状态": "待复验",
                "created_at": time.time(),
            }
            records.append(record)

    if write:
        path = relations_path(batch_key, root)
        merged_by_id: dict[str, dict[str, Any]] = {}
        for existing in read_jsonl(path):
            key = str(existing.get("relation_id") or (
                f"{batch_key}-{existing.get('terminal')}-"
                f"v{existing.get('old_version') or existing.get('original_version')}-"
                f"v{existing.get('new_version')}"
            ))
            merged_by_id[key] = existing
        for record in records:
            previous = merged_by_id.get(str(record["relation_id"]))
            # 重复核查同一映射时保留已经发生的复验结论，不把待办状态重置。
            if previous and previous.get("last_verification"):
                record["last_verification"] = previous["last_verification"]
                record["status"] = previous.get("status", record["status"])
                record["todo_status"] = previous.get("todo_status", record["todo_status"])
                record["复验状态"] = previous.get("复验状态", record["复验状态"])
            merged_by_id[str(record["relation_id"])] = record
        merged_records = list(merged_by_id.values())
        write_jsonl(path, merged_records)
        write_json(path.with_suffix(".json"), {
            "batch_key": batch_key,
            "total": len(merged_records),
            "items": merged_records,
            "relations": merged_records,
            "generated_at": time.time(),
        })
    return records


def mark_relation_verification(
    batch_key: str,
    terminal: str,
    verification: dict[str, Any],
    *,
    data_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    path = relations_path(batch_key, data_root)
    rows = read_jsonl(path)
    changed = False
    for row in rows:
        if str(row.get("terminal") or row.get("终端编号")) == str(terminal):
            row["last_verification"] = {
                "segment_version": verification.get("segment_version") or verification.get("version"),
                "emitted_segment_ids": verification.get("emitted_segment_ids") or verification.get("selected_segment_ids"),
                "mode": verification.get("mode"),
                "task_id": verification.get("task_id"),
                "verified_at": time.time(),
            }
            row["status"] = "已复验"
            row["todo_status"] = "reverified"
            row["复验状态"] = "已复验"
            changed = True
    if changed:
        write_jsonl(path, rows)
        write_json(path.with_suffix(".json"), {
            "batch_key": batch_key,
            "total": len(rows),
            "items": rows,
            "relations": rows,
            "updated_at": time.time(),
        })
    return rows


def verify_segment_source(
    batch_key: str,
    version: Any | None = None,
    *,
    expected_source_checksum: str | None = None,
    data_root: str | Path | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """核对并读取一个连贯分段版本，返回来源核对结论。"""
    result = read_segment_version(
        batch_key,
        version,
        expected_source_checksum=expected_source_checksum,
        data_root=data_root,
        **kwargs,
    )
    return {
        "ok": True,
        "batch_key": batch_key,
        "version": result["version"],
        "segment_version": result["version"],
        "source_checksum": result["source_checksum"],
        "segment_ids": result["segment_ids"],
        "segments": result["segments"],
        "file": result["file"],
    }


# 常见私有/公开命名别名。
_read_segment_version = read_segment_version
_load_segment_version = read_segment_version
read_versioned_segments = read_segment_version
load_versioned_segments = read_segment_version
read_segment_versions = list_segment_versions
