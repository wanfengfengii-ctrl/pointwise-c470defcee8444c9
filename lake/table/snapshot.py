from dataclasses import dataclass

from lake.ingest.validate import validate_text
from lake.table.catalog import load_catalog
from lake.table.rows import Row
from lake.table.segments import SegmentUnavailable, newest_mutations
from lake.table.storage import read_transaction, write_transaction


CURRENT_SNAPSHOT = 'current'


class SnapshotUnavailable(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class Snapshot:
    snapshot_id: str
    commit_no: int
    rows: tuple[Row, ...]


def visible_rows(db, sources, commit_no):
    return tuple(item.row for item in newest_mutations(db, sources, through=commit_no) if item.row is not None)


def _sources(db, snapshot_id):
    return tuple(row[0] for row in db.execute(
        'SELECT segment_id FROM snapshot_sources WHERE snapshot_id = ? ORDER BY segment_id', (snapshot_id,)
    ))


def _read_rows(db, snapshot_id, commit_no, sources):
    try:
        return visible_rows(db, sources, commit_no)
    except SegmentUnavailable as exc:
        raise SnapshotUnavailable(f'snapshot {snapshot_id!r} has an unavailable source') from exc


def snapshot(db, snapshot_id: str = CURRENT_SNAPSHOT) -> Snapshot:
    validate_text(snapshot_id, 'snapshot_id')
    with read_transaction(db):
        if snapshot_id == CURRENT_SNAPSHOT:
            catalog = load_catalog(db)
            commit_no, sources = catalog.head, catalog.segment_ids
        else:
            row = db.execute('SELECT commit_no FROM snapshots WHERE snapshot_id = ?', (snapshot_id,)).fetchone()
            if row is None:
                raise SnapshotUnavailable(f'unknown snapshot {snapshot_id!r}')
            commit_no, sources = row[0], _sources(db, snapshot_id)
        rows = _read_rows(db, snapshot_id, commit_no, sources)
        return Snapshot(snapshot_id, commit_no, rows)


def register_snapshot(db, snapshot_id: str) -> Snapshot:
    validate_text(snapshot_id, 'snapshot_id')
    if snapshot_id == CURRENT_SNAPSHOT:
        raise ValueError('current is reserved for the moving snapshot')
    with write_transaction(db):
        if db.execute('SELECT 1 FROM snapshots WHERE snapshot_id = ?', (snapshot_id,)).fetchone():
            raise ValueError(f'snapshot {snapshot_id!r} is already registered')
        catalog = load_catalog(db)
        rows = _read_rows(db, snapshot_id, catalog.head, catalog.segment_ids)
        db.execute('INSERT INTO snapshots VALUES (?, ?)', (snapshot_id, catalog.head))
        db.executemany(
            'INSERT INTO snapshot_sources VALUES (?, ?)',
            ((snapshot_id, segment_id) for segment_id in catalog.segment_ids),
        )
        return Snapshot(snapshot_id, catalog.head, rows)


def advance_current_snapshot(db, commit_no: int) -> None:
    updated = db.execute(
        'UPDATE lake_meta SET head = ? WHERE singleton = 1 AND head = ?', (commit_no, commit_no - 1)
    )
    if updated.rowcount != 1:
        raise ValueError('commit does not follow the current snapshot')


def snapshot_segment_dependencies(db) -> dict[int, tuple[str, ...]]:
    grouped = {}
    for segment_id, snapshot_id in db.execute(
        'SELECT segment_id, snapshot_id FROM snapshot_sources ORDER BY segment_id, snapshot_id'
    ):
        grouped.setdefault(segment_id, []).append(snapshot_id)
    return {segment_id: tuple(ids) for segment_id, ids in grouped.items()}


def rebind_snapshot_sources(db, original_ids: tuple[int, ...], new_id: int) -> tuple[str, ...]:
    selected = set(original_ids)
    rebound = []
    for snapshot_id, commit_no in db.execute('SELECT snapshot_id, commit_no FROM snapshots ORDER BY snapshot_id'):
        sources = set(_sources(db, snapshot_id))
        replaced = sources & selected
        if not replaced:
            continue
        candidate = (sources - replaced) | {new_id}
        before = _read_rows(db, snapshot_id, commit_no, sources)
        after = _read_rows(db, snapshot_id, commit_no, candidate)
        if before != after:
            continue
        db.executemany(
            'DELETE FROM snapshot_sources WHERE snapshot_id = ? AND segment_id = ?',
            ((snapshot_id, segment_id) for segment_id in sorted(replaced)),
        )
        db.execute('INSERT INTO snapshot_sources VALUES (?, ?)', (snapshot_id, new_id))
        rebound.append(snapshot_id)
    return tuple(rebound)
