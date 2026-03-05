from __future__ import annotations

import re
import uuid
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple

from .models import (
    DriftFinding,
    DriftType,
    MetricCluster,
    MetricInstance,
    Recommendation,
    RunMetadata,
)
from .recommendation import RecommendationInputs, choose_action, priority_score, rationale
from .scoring import DriftSignals, drift_severity_score

# Try to import advanced clustering; fall back to naive if unavailable
try:
    from .clustering import cluster_metrics_advanced
    _USE_ADVANCED_CLUSTERING = True
except ImportError:
    _USE_ADVANCED_CLUSTERING = False


def cluster_metrics(metrics: Iterable[MetricInstance]) -> List[MetricCluster]:
    grouped: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for metric in metrics:
        grouped[(metric.metric_name.lower(), metric.expression_signature)].append(metric.metric_id)

    clusters: List[MetricCluster] = []
    for idx, ((_name, _signature), members) in enumerate(grouped.items(), start=1):
        confidence = 0.95 if len(members) > 1 else 0.65
        clusters.append(MetricCluster(cluster_id=f"CL-{idx:04d}", members=members, confidence_score=confidence))
    return clusters


def detect_drift(cluster: MetricCluster, metric_lookup: Dict[str, MetricInstance]) -> List[DriftFinding]:
    findings: List[DriftFinding] = []
    members = [metric_lookup[mid] for mid in cluster.members]
    if len(members) < 2:
        return findings

    baseline = members[0]
    impacted_reports = sorted({m.report_id for m in members})

    filter_diff = any(set(m.filters) != set(baseline.filters) for m in members[1:])
    join_diff = any(m.join_path_signature != baseline.join_path_signature for m in members[1:])
    grain_diff = any(m.grain != baseline.grain for m in members[1:])

    formula_diff = len({m.expression_signature for m in members}) > 1

    signals = DriftSignals(
        formula_difference=100.0 if formula_diff else 0.0,
        filter_difference=100.0 if filter_diff else 0.0,
        join_difference=100.0 if join_diff else 0.0,
        grain_mismatch=100.0 if grain_diff else 0.0,
    )
    severity = drift_severity_score(signals)

    if formula_diff:
        findings.append(
            DriftFinding(
                finding_id=f"{cluster.cluster_id}-FORMULA",
                cluster_id=cluster.cluster_id,
                drift_type=DriftType.FORMULA,
                explanation="Cluster members use different normalized formula signatures.",
                severity_score=severity,
                impacted_reports=impacted_reports,
            )
        )
    if filter_diff:
        findings.append(
            DriftFinding(
                finding_id=f"{cluster.cluster_id}-FILTER",
                cluster_id=cluster.cluster_id,
                drift_type=DriftType.FILTER,
                explanation="Cluster members apply different filter sets (e.g., date/cash inclusion logic).",
                severity_score=severity,
                impacted_reports=impacted_reports,
            )
        )
    if join_diff:
        findings.append(
            DriftFinding(
                finding_id=f"{cluster.cluster_id}-JOIN",
                cluster_id=cluster.cluster_id,
                drift_type=DriftType.JOIN,
                explanation="Cluster members use different join paths/source objects.",
                severity_score=severity,
                impacted_reports=impacted_reports,
            )
        )
    if grain_diff:
        findings.append(
            DriftFinding(
                finding_id=f"{cluster.cluster_id}-GRAIN",
                cluster_id=cluster.cluster_id,
                drift_type=DriftType.GRAIN,
                explanation="Cluster members aggregate metrics at different grains.",
                severity_score=severity,
                impacted_reports=impacted_reports,
            )
        )
    return findings


def build_recommendations(
    clusters: List[MetricCluster],
    findings: List[DriftFinding],
    metric_lookup: Dict[str, MetricInstance],
) -> List[Recommendation]:
    findings_by_cluster: Dict[str, List[DriftFinding]] = defaultdict(list)
    for finding in findings:
        findings_by_cluster[finding.cluster_id].append(finding)

    output: List[Recommendation] = []
    for cluster in clusters:
        cluster_findings = findings_by_cluster.get(cluster.cluster_id, [])
        drift_severity = max((f.severity_score for f in cluster_findings), default=0.0)
        duplication_count = max(0, len(cluster.members) - 1)
        layer_violation = any("semantic view" not in obj.lower() for mid in cluster.members for obj in metric_lookup[mid].source_objects)

        inputs = RecommendationInputs(
            drift_severity=drift_severity,
            duplication_count=duplication_count,
            regulatory_sensitivity=50.0,
            usage_frequency=60.0,
            complexity=55.0,
            layer_violation=layer_violation,
        )
        action = choose_action(inputs)
        output.append(
            Recommendation(
                recommendation_id=f"REC-{cluster.cluster_id}",
                cluster_id=cluster.cluster_id,
                action_type=action,
                priority_score=priority_score(inputs),
                rationale=rationale(inputs, action),
                target_layer="snowflake_semantic" if action.value == "move_to_snowflake" else "bi_standardization",
            )
        )
    return output


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

_DRIFT_IMPACT: Dict[DriftType, str] = {
    DriftType.FORMULA: "Numbers will differ across reports using this metric",
    DriftType.FILTER: "Metric values will vary due to different inclusion/exclusion rules",
    DriftType.JOIN: "Underlying data scope differs, leading to inconsistent results",
    DriftType.GRAIN: "Aggregation level mismatch causes incomparable figures",
    DriftType.LAYER_VIOLATION: "Business logic computed in BI layer, risking inconsistency",
}

_DRIFT_REC_TEXT: Dict[DriftType, str] = {
    DriftType.FORMULA: "Align formula to canonical expression and publish via semantic layer",
    DriftType.FILTER: "Standardize filter conditions across all report variants",
    DriftType.JOIN: "Use common join path from governed data model",
    DriftType.GRAIN: "Define and enforce a standard grain for this KPI",
    DriftType.LAYER_VIOLATION: "Move computation to Snowflake semantic view",
}

_ACTION_AREA: Dict[str, str] = {
    "standardize": "KPI Standardization",
    "consolidate": "Report Consolidation",
    "retire": "Report Retirement",
    "rebuild": "Metric Rebuild",
    "move_to_snowflake": "Layer Governance",
}


def _priority_label(score: float) -> str:
    if score >= 70:
        return "P1 (0-30 days)"
    if score >= 40:
        return "P2 (30-90 days)"
    return "P3 (90+ days)"


def _infer_definition(expression: str) -> str:
    m = re.match(r"(\w+)\((\w+)\)", expression.strip().lower())
    if m:
        func, field = m.group(1), m.group(2)
        labels = {"sum": "Sum of", "avg": "Average of", "count": "Count of", "max": "Maximum", "min": "Minimum"}
        prefix = labels.get(func, func.capitalize() + " of")
        suffix = "records" if func == "count" else "values"
        return f"{prefix} {field} {suffix}"
    return f"Computed from: {expression}"


def _enrich_clusters(
    clusters: List[MetricCluster],
    metric_lookup: Dict[str, MetricInstance],
) -> List[MetricCluster]:
    for cluster in clusters:
        members_data = [metric_lookup[mid] for mid in cluster.members if mid in metric_lookup]
        if members_data:
            cluster.metric_intent = members_data[0].metric_name.title()
            cluster.reports = sorted({m.report_id for m in members_data})
            cluster.duplicate_count = max(0, len(cluster.members) - 1)
    return clusters


def _enrich_findings(
    findings: List[DriftFinding],
    clusters_by_id: Dict[str, MetricCluster],
) -> List[DriftFinding]:
    for finding in findings:
        cluster = clusters_by_id.get(finding.cluster_id)
        if cluster:
            finding.kpi_name = cluster.metric_intent
        finding.impact = _DRIFT_IMPACT.get(finding.drift_type, "Impact unknown")
        finding.recommendation_text = _DRIFT_REC_TEXT.get(finding.drift_type, "Review and standardize")
    return findings


def _enrich_recommendations(
    recommendations: List[Recommendation],
    clusters_by_id: Dict[str, MetricCluster],
) -> List[Recommendation]:
    for rec in recommendations:
        rec.area = _ACTION_AREA.get(rec.action_type.value, "General Governance")
        rec.priority_label = _priority_label(rec.priority_score)
        cluster = clusters_by_id.get(rec.cluster_id)
        if cluster:
            rec.impacted_reports = cluster.reports
    return recommendations


def _build_report_inventory(metrics: List[MetricInstance]) -> List[dict]:
    report_datasets: Dict[str, set] = defaultdict(set)
    for m in metrics:
        report_datasets[m.report_id].add(m.dataset_id)

    inventory = []
    for report_id, datasets in sorted(report_datasets.items()):
        inventory.append({
            "report_id": report_id,
            "name": report_id.replace("_", " ").title(),
            "folder": "Analytics",
            "owner": None,
            "last_modified": None,
            "dataset_count": len(datasets),
            "primary_domain": None,
        })
    return inventory


def _build_kpi_dictionary(metrics: List[MetricInstance]) -> List[dict]:
    kpi_data: Dict[str, dict] = {}
    for m in metrics:
        key = m.metric_name.lower()
        if key not in kpi_data:
            primary_source = m.source_objects[0] if m.source_objects else "Unknown"
            computed_layer = "Semantic Layer" if any("semantic" in obj.lower() for obj in m.source_objects) else "BI Layer"
            kpi_data[key] = {
                "kpi_name": m.metric_name,
                "inferred_definition": _infer_definition(m.expression_signature),
                "grain": m.grain,
                "primary_source": primary_source,
                "computed_in_layer": computed_layer,
                "found_in_reports": set(),
                "risk_notes": "Review for standardization",
            }
        kpi_data[key]["found_in_reports"].add(m.report_id)

    result = []
    for entry in kpi_data.values():
        reports = sorted(entry["found_in_reports"])
        risk = "Multiple variants detected" if len(reports) > 1 else entry["risk_notes"]
        result.append({
            "kpi_name": entry["kpi_name"],
            "inferred_definition": entry["inferred_definition"],
            "grain": entry["grain"],
            "primary_source": entry["primary_source"],
            "computed_in_layer": entry["computed_in_layer"],
            "found_in_reports": reports,
            "risk_notes": risk,
        })
    return result


_HOW_TO_READ = [
    {
        "section": "Executive Scorecard",
        "content": (
            "High-level counts of reports scanned, KPIs inferred, clusters formed, drift findings, "
            "P1 recommendations, and the highest severity score. Use this as the starting point for any "
            "stakeholder briefing."
        ),
    },
    {
        "section": "Report Inventory",
        "content": (
            "A row per unique report ID discovered in the input data. Shows the report name, folder, "
            "owner, last modified date, number of datasets, and primary business domain."
        ),
    },
    {
        "section": "KPI Dictionary",
        "content": (
            "One entry per unique KPI name. Includes the inferred definition (derived from the expression "
            "signature), the granularity, primary data source, the layer where it is computed, and any "
            "risk notes such as multiple variant detection."
        ),
    },
    {
        "section": "Metric Clusters",
        "content": (
            "Groups of metric instances the engine determined represent the same underlying business concept. "
            "Confidence score reflects how similar the instances are. Duplicate count = cluster size - 1."
        ),
    },
    {
        "section": "Drift Findings",
        "content": (
            "Each row is a specific difference detected within a cluster: formula, filter, join path, or "
            "grain mismatch. The severity score (0-100) reflects business impact. Impact and recommendation "
            "text guide remediation."
        ),
    },
    {
        "section": "Recommendations Backlog",
        "content": (
            "Actionable items ranked by priority score. P1 items (score >= 70) should be addressed within "
            "0-30 days; P2 within 30-90 days; P3 at 90+ days. Each recommendation maps to a specific "
            "action type and target layer."
        ),
    },
    {
        "section": "How to Read",
        "content": (
            "This sheet. Contains guidance on interpreting every other sheet in this workbook. Share with "
            "stakeholders who are new to the AI Metric Drift Detection output."
        ),
    },
]


def run_analysis(metrics: Iterable[MetricInstance], use_advanced_clustering: bool = True) -> dict:
    metrics = list(metrics)
    metric_lookup = {m.metric_id: m for m in metrics}

    # Use advanced AI clustering if available, else fall back to naive
    if use_advanced_clustering and _USE_ADVANCED_CLUSTERING:
        clusters = cluster_metrics_advanced(metrics)
    else:
        clusters = cluster_metrics(metrics)

    # Enrich clusters before drift detection so reports list is populated
    clusters = _enrich_clusters(clusters, metric_lookup)
    clusters_by_id = {c.cluster_id: c for c in clusters}

    findings = [finding for cluster in clusters for finding in detect_drift(cluster, metric_lookup)]
    findings = _enrich_findings(findings, clusters_by_id)

    recommendations = build_recommendations(clusters, findings, metric_lookup)
    recommendations = _enrich_recommendations(recommendations, clusters_by_id)

    # Executive Scorecard
    p1_count = sum(1 for r in recommendations if r.priority_label and r.priority_label.startswith("P1"))
    highest_severity = max((f.severity_score for f in findings), default=0.0)
    unique_datasets = {m.dataset_id for m in metrics}
    unique_kpis = {m.metric_name.lower() for m in metrics}
    unique_reports = {m.report_id for m in metrics}

    metadata = RunMetadata(
        run_id=str(uuid.uuid4()),
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        reports_scanned=len(unique_reports),
        datasets_extracted=len(unique_datasets),
        kpis_inferred=len(unique_kpis),
        clusters_formed=len(clusters),
        drift_findings_count=len(findings),
        p1_recommendations=p1_count,
        highest_severity=round(highest_severity, 2),
    )

    return {
        "executive_scorecard": asdict(metadata),
        "report_inventory": _build_report_inventory(metrics),
        "kpi_dictionary": _build_kpi_dictionary(metrics),
        "clusters": [asdict(c) for c in clusters],
        "drift_findings": [asdict(f) for f in findings],
        "recommendations": [asdict(r) for r in recommendations],
        "how_to_read": _HOW_TO_READ,
    }
