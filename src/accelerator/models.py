from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class DriftType(str, Enum):
    FORMULA = "formula"
    FILTER = "filter"
    JOIN = "join"
    GRAIN = "grain"
    LAYER_VIOLATION = "layer_violation"


class ActionType(str, Enum):
    STANDARDIZE = "standardize"
    CONSOLIDATE = "consolidate"
    RETIRE = "retire"
    REBUILD = "rebuild"
    MOVE_TO_SNOWFLAKE = "move_to_snowflake"


@dataclass
class Report:
    report_id: str
    name: str
    folder: str
    owner: Optional[str] = None
    last_modified: Optional[str] = None
    dataset_count: int = 0
    primary_domain: Optional[str] = None


@dataclass
class Dataset:
    dataset_id: str
    report_id: str
    name: str
    sql_text: str
    data_source: Optional[str] = None
    parameters: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricInstance:
    metric_id: str
    report_id: str
    dataset_id: str
    metric_name: str
    expression_signature: str
    grain: str
    filters: List[str]
    join_path_signature: str
    source_objects: List[str]
    parameters_impacting_metric: List[str] = field(default_factory=list)
    embedding_vector: Optional[List[float]] = None


@dataclass
class MetricCluster:
    cluster_id: str
    members: List[str]
    confidence_score: float
    metric_intent: Optional[str] = None
    reports: List[str] = field(default_factory=list)
    duplicate_count: int = 0


@dataclass
class DriftFinding:
    finding_id: str
    cluster_id: str
    drift_type: DriftType
    explanation: str
    severity_score: float
    impacted_reports: List[str]
    kpi_name: Optional[str] = None
    impact: Optional[str] = None
    recommendation_text: Optional[str] = None


@dataclass
class Recommendation:
    recommendation_id: str
    cluster_id: str
    action_type: ActionType
    priority_score: float
    rationale: str
    target_layer: str
    owner: Optional[str] = None
    area: Optional[str] = None
    impacted_reports: List[str] = field(default_factory=list)
    priority_label: Optional[str] = None


@dataclass
class RunSettings:
    scope: str = "all_reports"
    drift_sensitivity: str = "balanced"
    include_usage_signals: bool = True
    include_regulatory_tags: bool = True


@dataclass
class RunSummary:
    run_id: str
    reports_scanned: int
    metrics_inferred: int
    clusters_created: int
    drift_findings: int
    recommendations_created: int


@dataclass
class RunMetadata:
    run_id: str
    generated_at: str
    reports_scanned: int
    datasets_extracted: int
    kpis_inferred: int
    clusters_formed: int
    drift_findings_count: int
    p1_recommendations: int
    highest_severity: float
