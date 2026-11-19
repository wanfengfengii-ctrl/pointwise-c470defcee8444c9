from dataclasses import dataclass

from lake.history.records import CommitEvidence
from lake.table.rows import Row


@dataclass(frozen=True, slots=True)
class Change:
    commit_no: int
    row_id: str
    action: str
    before: Row | None
    after: Row | None

    @property
    def position(self) -> tuple[int, str]:
        return self.commit_no, self.row_id


def project_partition(frame: CommitEvidence, partition: str) -> tuple[Change, ...]:
    changes = []
    for item in frame.changes:
        before = item.before if item.before is not None and item.before.partition == partition else None
        after = item.after if item.after is not None and item.after.partition == partition else None
        if before == after:
            continue
        if before is None:
            action = 'add'
        elif after is None:
            action = 'remove'
        else:
            action = 'update'
        changes.append(Change(frame.commit_no, item.row_id, action, before, after))
    return tuple(changes)
