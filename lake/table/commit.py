from lake.history.store import register_source_commit
from lake.ingest.batch import BatchRequest
from lake.table.catalog import append_segment, load_catalog
from lake.table.rows import read_current_rows, settle_rows, write_settlement
from lake.table.segments import SegmentMutation, SegmentPayload, write_segment
from lake.table.snapshot import advance_current_snapshot
from lake.table.storage import MAX_SQLITE_INTEGER


def commit_batch(db, request: BatchRequest) -> int:
    """Settle one validated request inside the caller's write transaction."""
    head = load_catalog(db).head
    if head == MAX_SQLITE_INTEGER:
        raise OverflowError('the lake has exhausted its commit numbers')
    commit_no = head + 1
    starting_rows = read_current_rows(db, (edit.row_id for edit in request.edits))
    settlement = settle_rows({row.row_id: row for row in starting_rows}, request.edits)
    mutations = tuple(
        SegmentMutation(row_id, commit_no, after)
        for row_id, _, after in settlement.net_changes()
    )
    segment_id = write_segment(db, SegmentPayload(mutations, commit_no, starting_rows, request.edits))
    register_source_commit(db, commit_no, segment_id)
    write_settlement(db, settlement)
    append_segment(db, segment_id)
    advance_current_snapshot(db, commit_no)
    return commit_no
