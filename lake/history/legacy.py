from lake.history.records import CommitEvidence, make_evidence
from lake.table.rows import settle_rows
from lake.table.segments import SegmentMutation, SegmentPayload, SegmentUnavailable


def restore_segment_commits(payload: SegmentPayload) -> tuple[CommitEvidence, ...]:
    if payload.source_commit is None:
        raise SegmentUnavailable('a compacted segment is not an original write source')
    starting_rows = {row.row_id: row for row in payload.starting_rows}
    settlement = settle_rows(starting_rows, payload.edits)
    frame = make_evidence(payload.source_commit, settlement)
    contributions = tuple(
        SegmentMutation(item.row_id, frame.commit_no, item.after) for item in frame.changes
    )
    if contributions != payload.mutations:
        raise SegmentUnavailable('original edits do not match the segment contributions')
    return (frame,)
