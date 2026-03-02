from __future__ import annotations

from dataclasses import dataclass

from .models import ActionType


@dataclass
class RecommendationInputs:
    drift_severity: float
    duplication_count: int
    regulatory_sensitivity: float
    usage_frequency: float
    complexity: float
    layer_violation: bool = False


def choose_action(inputs: RecommendationInputs) -> ActionType:
    if inputs.layer_violation:
        return ActionType.MOVE_TO_SNOWFLAKE
    if inputs.drift_severity >= 80 and inputs.regulatory_sensitivity >= 70:
        return ActionType.REBUILD
    if inputs.duplication_count >= 3 and inputs.drift_severity < 50:
        return ActionType.CONSOLIDATE
    if inputs.usage_frequency < 25 and inputs.duplication_count >= 2:
        return ActionType.RETIRE
    return ActionType.STANDARDIZE


def priority_score(inputs: RecommendationInputs) -> float:
    raw = (
        inputs.drift_severity * 0.30
        + min(inputs.duplication_count * 20, 100) * 0.20
        + inputs.regulatory_sensitivity * 0.20
        + inputs.usage_frequency * 0.15
        + inputs.complexity * 0.15
    )
    return round(max(0.0, min(raw, 100.0)), 2)


def rationale(inputs: RecommendationInputs, action: ActionType) -> str:
    if action == ActionType.MOVE_TO_SNOWFLAKE:
        return "Metric is recomputed in BI layer; centralize logic in governed Snowflake semantic view."
    if action == ActionType.REBUILD:
        return "High drift with high regulatory sensitivity creates material reporting risk."
    if action == ActionType.CONSOLIDATE:
        return "Low drift but high duplication indicates merge opportunity across reports."
    if action == ActionType.RETIRE:
        return "Redundant low-usage report can be retired after stakeholder confirmation."
    return "Adopt canonical KPI definition and align filters/joins across report variants."
