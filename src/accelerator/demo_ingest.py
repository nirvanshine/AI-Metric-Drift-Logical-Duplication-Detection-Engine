from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List

from .models import MetricInstance

AGG_PATTERN = re.compile(
    r"(?P<expr>(SUM|AVG|COUNT|MIN|MAX)\s*\([^\)]+\)|[A-Za-z_][\w]*\s*/\s*[A-Za-z_][\w]*)\s*(?:AS\s+)?(?P<alias>[A-Za-z_][\w]*)?",
    re.IGNORECASE,
)


def _signature(expression: str) -> str:
    normalized = re.sub(r"\s+", "", expression.lower())
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]


def _extract_filters(sql_text: str) -> List[str]:
    where_match = re.search(r"\bwhere\b(?P<where>.+?)(\bgroup\b|\border\b|$)", sql_text, flags=re.IGNORECASE | re.DOTALL)
    if not where_match:
        return []
    where_text = where_match.group("where")
    parts = [part.strip() for part in re.split(r"\band\b", where_text, flags=re.IGNORECASE) if part.strip()]
    return parts


def _join_signature(sql_text: str) -> str:
    joins = re.findall(r"\bjoin\s+([A-Za-z_][\w\.]*)", sql_text, flags=re.IGNORECASE)
    from_match = re.search(r"\bfrom\s+([A-Za-z_][\w\.]*)", sql_text, flags=re.IGNORECASE)
    path = [from_match.group(1)] if from_match else []
    path.extend(joins)
    return "->".join(path) if path else "unknown"


def extract_metrics_from_sql(report_id: str, dataset_id: str, sql_text: str) -> List[MetricInstance]:
    select_match = re.search(r"\bselect\b(?P<select>.+?)\bfrom\b", sql_text, flags=re.IGNORECASE | re.DOTALL)
    if not select_match:
        return []

    metrics: List[MetricInstance] = []
    select_text = select_match.group("select")
    filters = _extract_filters(sql_text)
    join_signature = _join_signature(sql_text)
    sources = re.findall(r"\bfrom\s+([A-Za-z_][\w\.]*)", sql_text, flags=re.IGNORECASE)
    sources.extend(re.findall(r"\bjoin\s+([A-Za-z_][\w\.]*)", sql_text, flags=re.IGNORECASE))

    for idx, match in enumerate(AGG_PATTERN.finditer(select_text), start=1):
        expr = match.group("expr")
        alias = match.group("alias") or f"metric_{idx}"
        metrics.append(
            MetricInstance(
                metric_id=f"{report_id}:{dataset_id}:{idx}",
                report_id=report_id,
                dataset_id=dataset_id,
                metric_name=alias,
                expression_signature=_signature(expr),
                grain="unknown",
                filters=filters,
                join_path_signature=join_signature,
                source_objects=sources or ["unknown"],
            )
        )
    return metrics


def parse_report_upload(json_payload: str) -> List[MetricInstance]:
    data: Dict[str, Any] = json.loads(json_payload)
    reports = data.get("reports", [])
    metrics: List[MetricInstance] = []
    for report in reports:
        report_id = report["report_id"]
        for dataset in report.get("datasets", []):
            sql_text = dataset.get("sql_text", "")
            dataset_id = dataset.get("dataset_id", dataset.get("name", "dataset"))
            metrics.extend(extract_metrics_from_sql(report_id, dataset_id, sql_text))
    return metrics
