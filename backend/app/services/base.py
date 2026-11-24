"""模块服务基类：共享查询、登记、动作、文件IO和跨模块数据流骨架。"""
from __future__ import annotations

import json
import os
import hashlib
import math
import time
from pathlib import Path
from typing import Any

from app.store import store

DATA_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "data"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    OPERATION_TERM = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def __init__(self, data_root: str | Path | None = None, *, data_dir: str | Path | None = None, **kwargs: Any) -> None:
        # 允许测试/调用方为单个服务实例指定产物根目录；不传时沿用全局 DATA_ROOT。
        root = data_root or data_dir or kwargs.get("root")
        self._data_root_override = Path(root) if root is not None else None

    # -- 基础 CRUD --

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }

    # -- 文件 IO 工具 --

    def _data_dir(self, sub: str) -> Path:
        override = self.__dict__.get("_data_root_override")
        root = Path(override) if override is not None else DATA_ROOT
        p = root / sub
        _ensure_dir(p)
        return p

    def _write_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """将记录列表以JSONL格式追加写入指定文件，返回写入条数。"""
        _ensure_dir(filepath.parent)
        written = 0
        with open(filepath, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                written += 1
        return written

    def _write_jsonl_overwrite(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """覆盖写入完整JSONL产物，避免重建批次时把多版输入重复追加。"""
        _ensure_dir(filepath.parent)
        with open(filepath, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        return len(records)

    def _read_jsonl(self, filepath: Path) -> list[dict[str, Any]]:
        """从JSONL文件读取全部记录并返回字典列表。"""
        if not filepath.exists():
            return []
        records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def _write_checkpoint(self, name: str, state: dict[str, Any]) -> Path:
        """将状态数据写入检查点文件，供跨模块恢复中断处理。"""
        p = self._data_dir("checkpoints")
        cp_file = p / f"{name}.json"
        state["_updated_at"] = time.time()
        with open(cp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        return cp_file

    def _read_checkpoint(self, name: str) -> dict[str, Any] | None:
        """读取检查点文件返回状态数据，不存在时返回None。"""
        cp_file = self._data_dir("checkpoints") / f"{name}.json"
        if not cp_file.exists():
            return None
        with open(cp_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _compute_checksum(self, content: str) -> str:
        """计算字符串内容的SHA256校验值。"""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    # -- 跨模块自动触发帮助器 --

    @staticmethod
    def _safe_import(module_path: str, class_name: str):
        """延迟导入目标模块的指定类，避免循环依赖。"""
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)

    def _auto_create_seed(self, module: str, overrides: dict[str, Any]) -> dict[str, Any] | None:
        """跨模块在目标模块中自动创建配套配置条目并写入数据集。"""
        rows = store.rows(module)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        entry = {"id": new_id}
        for field, default_val in overrides.items():
            entry[field] = default_val
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False
        rows.append(entry)
        return entry

    # -- 地理空间工具函数 --

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """计算两坐标点的Haversine球面距离（米）。"""
        R = 6371000.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    @staticmethod
    def _point_in_circle(px: float, py: float, cx: float, cy: float, r: float) -> bool:
        """判断点(px,py)是否在以(cx,cy)为圆心、r为半径的圆内。"""
        return math.sqrt((px - cx) ** 2 + (py - cy) ** 2) <= r

    @staticmethod
    def _geohash_encode(lat: float, lon: float, precision: int = 8) -> str:
        """将经纬度编码为指定精度的geohash字符串，用于网格索引生成。"""
        base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
        lat_range = (-90.0, 90.0)
        lon_range = (-180.0, 180.0)
        is_even = True
        bit = 0
        ch = 0
        geohash = ""
        while len(geohash) < precision:
            if is_even:
                mid = (lon_range[0] + lon_range[1]) / 2
                if lon >= mid:
                    ch |= 1 << (4 - bit)
                    lon_range = (mid, lon_range[1])
                else:
                    lon_range = (lon_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if lat >= mid:
                    ch |= 1 << (4 - bit)
                    lat_range = (mid, lat_range[1])
                else:
                    lat_range = (lat_range[0], mid)
            is_even = not is_even
            bit += 1
            if bit == 5:
                geohash += base32[ch]
                bit = 0
                ch = 0
        return geohash

    def _generate_sample_points(self, count: int = 50, center_lat: float = 39.9042,
                                  center_lon: float = 116.4074, radius: float = 0.05) -> list[dict[str, Any]]:
        """从中心坐标生成指定数量的模拟定位点数据，用于测试数据流。"""
        import random
        import hashlib
        points = []
        for i in range(count):
            lat = center_lat + random.uniform(-radius, radius)
            lon = center_lon + random.uniform(-radius, radius)
            ts = time.time() - random.randint(0, 86400)
            points.append({
                "point_id": i + 1,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "timestamp": ts,
                "accuracy": round(random.uniform(1.0, 50.0), 1),
                "bearing": round(random.uniform(0, 360), 1),
                "speed": round(random.uniform(0, 80), 2),
                "terminal": f"TERM-{hashlib.md5(str(i).encode()).hexdigest()[:4]}",
            })
        return points