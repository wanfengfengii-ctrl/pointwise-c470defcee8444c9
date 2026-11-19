from dataclasses import dataclass

from lake.table.rows import Row, RowSettlement


@dataclass(frozen=True, slots=True)
class RowEvidence:
    row_id: str
    before: Row | None
    after: Row | None


@dataclass(frozen=True, slots=True)
class CommitEvidence:
    commit_no: int
    changes: tuple[RowEvidence, ...]


def make_evidence(commit_no: int, settlement: RowSettlement) -> CommitEvidence:
    return CommitEvidence(commit_no, tuple(
        RowEvidence(row_id, before, after) for row_id, before, after in settlement.net_changes()
    ))
