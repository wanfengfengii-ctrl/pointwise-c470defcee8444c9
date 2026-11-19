from dataclasses import dataclass
from typing import Iterable

from lake.history.store import history_segment_dependencies
from lake.table.catalog import load_catalog
from lake.table.snapshot import snapshot_segment_dependencies


@dataclass(frozen=True, slots=True)
class RetirementEligibility:
    segment_id: int
    reasons: tuple[str, ...]

    @property
    def eligible(self) -> bool:
        return not self.reasons


def assess_retirement(db, segment_ids: Iterable[int]) -> tuple[RetirementEligibility, ...]:
    active = set(load_catalog(db).segment_ids)
    snapshots = snapshot_segment_dependencies(db)
    history = history_segment_dependencies(db)
    results = []
    for segment_id in segment_ids:
        reasons = []
        if segment_id in active:
            reasons.append('active_catalog')
        if segment_id in snapshots:
            reasons.append('fixed_snapshot')
        if segment_id in history:
            reasons.append('retained_history')
        results.append(RetirementEligibility(segment_id, tuple(reasons)))
    return tuple(results)
