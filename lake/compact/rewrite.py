from dataclasses import dataclass

from lake.compact.plan import CompactionPlan, validate_plan
from lake.history.records import CommitEvidence
from lake.history.store import read_history_bounds, read_segment_evidence
from lake.table.segments import SegmentPayload, newest_mutations
from lake.table.storage import read_transaction


@dataclass(frozen=True, slots=True)
class PreparedReplacement:
    plan: CompactionPlan
    payload: SegmentPayload
    evidence: tuple[tuple[int, CommitEvidence], ...]


def prepare_replacement(db, plan: CompactionPlan) -> PreparedReplacement:
    with read_transaction(db):
        validate_plan(db, plan)
        payload = SegmentPayload(newest_mutations(db, plan.segment_ids))
        floor, head = read_history_bounds(db)
        evidence = tuple(
            (segment_id, frame)
            for segment_id in plan.segment_ids
            for frame in read_segment_evidence(db, segment_id, floor, head)
        )
        return PreparedReplacement(plan, payload, evidence)
