from dataclasses import dataclass
from typing import Iterable

from lake.ingest.batch import Edit
from lake.table.rows import Row


class SegmentUnavailable(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class SegmentMutation:
    row_id: str
    commit_no: int
    row: Row | None


@dataclass(frozen=True, slots=True)
class SegmentPayload:
    mutations: tuple[SegmentMutation, ...]
    source_commit: int | None = None
    starting_rows: tuple[Row, ...] = ()
    edits: tuple[Edit, ...] = ()


def _image_fields(row):
    if row is None:
        return None, None
    return row.partition, format(row.reading, 'x')


def write_segment(db, payload: SegmentPayload) -> int:
    if payload.source_commit is None and (payload.starting_rows or payload.edits):
        raise ValueError('original edits require a source commit')
    header = db.execute(
        'INSERT INTO segments (source_commit, base_count, edit_count) VALUES (?, ?, ?)',
        (payload.source_commit, len(payload.starting_rows), len(payload.edits)),
    )
    segment_id = header.lastrowid
    db.executemany(
        '''INSERT INTO segment_mutations (segment_id, row_id, commit_no, partition_name, reading)
        VALUES (?, ?, ?, ?, ?)''',
        ((segment_id, item.row_id, item.commit_no, *_image_fields(item.row)) for item in payload.mutations),
    )
    db.executemany(
        'INSERT INTO segment_base (segment_id, row_id, partition_name, reading) VALUES (?, ?, ?, ?)',
        ((segment_id, row.row_id, *_image_fields(row)) for row in payload.starting_rows),
    )
    db.executemany(
        '''INSERT INTO segment_edits (segment_id, ordinal, operation, row_id, partition_name, reading)
        VALUES (?, ?, ?, ?, ?, ?)''',
        ((segment_id, ordinal, edit.operation, edit.row_id, edit.partition,
          None if edit.reading is None else format(edit.reading, 'x'))
         for ordinal, edit in enumerate(payload.edits)),
    )
    return segment_id


def _header(db, segment_id):
    header = db.execute(
        'SELECT source_commit, base_count, edit_count FROM segments WHERE segment_id = ?', (segment_id,)
    ).fetchone()
    if header is None:
        raise SegmentUnavailable(f'segment {segment_id} is unavailable')
    return header


def _mutations(db, segment_id):
    records = db.execute(
        '''SELECT row_id, commit_no, partition_name, reading FROM segment_mutations
        WHERE segment_id = ? ORDER BY row_id''', (segment_id,),
    )
    return tuple(
        SegmentMutation(key, commit, None if partition is None else Row(key, partition, int(value, 16)))
        for key, commit, partition, value in records
    )


def read_segment_mutations(db, segment_id: int) -> tuple[SegmentMutation, ...]:
    _header(db, segment_id)
    return _mutations(db, segment_id)


def read_segment(db, segment_id: int) -> SegmentPayload:
    source, base_count, edit_count = _header(db, segment_id)
    mutations = _mutations(db, segment_id)
    if source is None:
        return SegmentPayload(mutations)
    base = tuple(Row(key, partition, int(value, 16)) for key, partition, value in db.execute(
        '''SELECT row_id, partition_name, reading FROM segment_base
        WHERE segment_id = ? ORDER BY row_id''', (segment_id,),
    ))
    entries = db.execute(
        '''SELECT ordinal, operation, row_id, partition_name, reading FROM segment_edits
        WHERE segment_id = ? ORDER BY ordinal''', (segment_id,),
    ).fetchall()
    if len(base) != base_count or len(entries) != edit_count:
        raise SegmentUnavailable(f'original write material for segment {segment_id} is incomplete')
    edits = []
    for position, (ordinal, operation, key, partition, value) in enumerate(entries):
        if ordinal != position:
            raise SegmentUnavailable(f'edit order for segment {segment_id} is incomplete')
        edits.append(Edit(operation, key, partition, None if value is None else int(value, 16)))
    return SegmentPayload(mutations, source, base, tuple(edits))


def delete_segment(db, segment_id: int) -> None:
    removed = db.execute('DELETE FROM segments WHERE segment_id = ?', (segment_id,))
    if removed.rowcount != 1:
        raise SegmentUnavailable(f'segment {segment_id} is unavailable')


def newest_mutations(
    db, segment_ids: Iterable[int], *, through: int | None = None,
) -> tuple[SegmentMutation, ...]:
    newest = {}
    for segment_id in segment_ids:
        for item in read_segment_mutations(db, segment_id):
            if through is not None and item.commit_no > through:
                continue
            previous = newest.get(item.row_id)
            if previous is None or item.commit_no > previous.commit_no:
                newest[item.row_id] = item
            elif item.commit_no == previous.commit_no and item != previous:
                raise ValueError(f'conflicting contributions for {item.row_id!r} in commit {item.commit_no}')
    return tuple(newest[key] for key in sorted(newest))
