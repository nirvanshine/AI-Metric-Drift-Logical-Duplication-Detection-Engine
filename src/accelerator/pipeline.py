from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from typing import Dict, Iterable, List, Tuple

from .models import DriftFinding, DriftType, MetricCluster, MetricInstance, Recommendation
from .recommendation import RecommendationInputs, choose_action, priority_score, rationale
from .scoring import DriftSignals, drift_severity_score


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


def run_analysis(metrics: Iterable[MetricInstance]) -> dict:
    metrics = list(metrics)
    metric_lookup = {m.metric_id: m for m in metrics}
    clusters = cluster_metrics(metrics)
    findings = [finding for cluster in clusters for finding in detect_drift(cluster, metric_lookup)]
    recommendations = build_recommendations(clusters, findings, metric_lookup)

    return {
        "clusters": [asdict(c) for c in clusters],
        "drift_findings": [asdict(f) for f in findings],
        "recommendations": [asdict(r) for r in recommendations],
    }
