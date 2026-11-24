"""坐标标准化领域服务：维护坐标系转换规则、偏移参数与越界处理，并对采集批次执行坐标归一化。"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CoordTransformService(BaseModuleService):
    MODULE = "coord_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源坐标系", "目标坐标系", "偏移参数", "精度位数", "越界处理", "规则版本", "生效批次"]
    REQUIRED_FIELDS = ["规则编号", "源坐标系", "目标坐标系"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    def transform_batch(self, batch_key: str, rule_id: int = 1) -> dict[str, Any]:
        """读取采集批次定位点文件，解析原始坐标并执行坐标系转换，生成转换后的定位点数据集文件。"""
        points_file = self._data_dir("geo_points") / f"{batch_key}.jsonl"
        if not points_file.exists():
            raise FileNotFoundError(f"批次 {batch_key} 的定位点数据文件不存在，请先接入采集批次")
        points = self._read_jsonl(points_file)

        # 获取转换规则
        rule = store.find(self.MODULE, rule_id)
        offset_lat = 0.0
        offset_lon = 0.0
        precision = 6
        if rule:
            try:
                offset_params = rule.get("偏移参数", "0.0,0.0")
                parts = offset_params.split(",")
                offset_lat = float(parts[0]) if len(parts) > 0 else 0.0
                offset_lon = float(parts[1]) if len(parts) > 1 else 0.0
                precision = int(rule.get("精度位数", "6"))
            except (ValueError, IndexError):
                pass

        transformed = []
        for p in points:
            new_lat = round(p["lat"] + offset_lat, precision)
            new_lon = round(p["lon"] + offset_lon, precision)
            # 越界处理
            if new_lat > 90 or new_lat < -90:
                new_lat = max(-90, min(90, new_lat))
            if new_lon > 180 or new_lon < -180:
                new_lon = max(-180, min(180, new_lon))
            transformed.append({
                "point_id": p["point_id"],
                "batch_key": batch_key,
                "orig_lat": p["lat"],
                "orig_lon": p["lon"],
                "trans_lat": new_lat,
                "trans_lon": new_lon,
                "timestamp": p.get("timestamp"),
                "terminal": p.get("terminal"),
                "accuracy": p.get("accuracy"),
                "bearing": p.get("bearing"),
                "speed": p.get("speed"),
                "rule_version": rule.get("规则版本", "v1.0") if rule else "v1.0",
            })

        output_file = self._data_dir("coord_transformed") / f"{batch_key}_transformed.jsonl"
        written = self._write_jsonl_overwrite(output_file, transformed)

        # 更新记录条数
        if rule:
            rule["生效批次"] = batch_key

        return {
            "batch_key": batch_key,
            "input_points": len(points),
            "transformed_points": written,
            "rule_id": rule_id,
            "output_file": str(output_file),
        }

    def get_transformed_points(self, batch_key: str) -> tuple[list[dict[str, Any]], str]:
        """读取指定批次的坐标转换结果数据集，返回转换后的定位点记录和文件路径。"""
        file_path = self._data_dir("coord_transformed") / f"{batch_key}_transformed.jsonl"
        if not file_path.exists():
            raise FileNotFoundError(f"批次 {batch_key} 的转换数据集文件不存在")
        points = self._read_jsonl(file_path)
        return points, str(file_path)

    def apply_rule_to_all(self, rule_id: int) -> dict[str, Any]:
        """对已接入的全部采集批次应用指定转换规则，批量生成坐标标准化数据集。"""
        rule = store.find(self.MODULE, rule_id)
        if rule is None:
            return {"error": f"转换规则 {rule_id} 不存在"}
        geo_svc = self._safe_import("app.services.geo_source", "GeoSourceService")()
        active_batches = geo_svc.list_active_batches()
        results = {}
        for batch in active_batches:
            batch_key = batch["批次编号"]
            try:
                results[batch_key] = self.transform_batch(batch_key, rule_id)
            except Exception as e:
                results[batch_key] = {"error": str(e)}
        return {"rule_id": rule_id, "batches_processed": len(results), "results": results}