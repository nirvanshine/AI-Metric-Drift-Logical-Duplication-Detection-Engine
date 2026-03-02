from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DriftSignals:
    formula_difference: float
    filter_difference: float
    join_difference: float
    grain_mismatch: float


@dataclass
class ConsolidationSignals:
    usage: float
    complexity: float
    drift_severity: float
    duplicates: float
    regulatory: float


def _clamp(score: float) -> float:
    return max(0.0, min(100.0, round(score, 2)))


def drift_severity_score(signals: DriftSignals) -> float:
    """Weighted model from product requirements: 40/25/20/15."""
    weighted = (
        signals.formula_difference * 0.40
        + signals.filter_difference * 0.25
        + signals.join_difference * 0.20
        + signals.grain_mismatch * 0.15
    )
    return _clamp(weighted)


def consolidation_priority_score(signals: ConsolidationSignals) -> float:
    """Weighted model from product requirements: 30/20/20/15/15."""
    weighted = (
        signals.usage * 0.30
        + signals.complexity * 0.20
        + signals.drift_severity * 0.20
        + signals.duplicates * 0.15
        + signals.regulatory * 0.15
    )
    return _clamp(weighted)
