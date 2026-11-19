import json

from lake.history.legacy import restore_segment_commits
from lake.history.records import CommitEvidence, RowEvidence
from lake.table.rows import Row
from lake.table.segments import SegmentUnavailable, read_segment


class HistoryUnavailable(LookupError):
    pass


def register_source_commit(db, commit_no: int, segment_id: int) -> None:
    db.execute('INSERT INTO commit_history VALUES (?, ?)', (commit_no, segment_id))


def read_history_bounds(db) -> tuple[int, int]:
    origin, head, retention = db.execute(
        'SELECT origin, head, retention FROM lake_meta WHERE singleton = 1'
    ).fetchone()
    return max(origin, head - retention), head


def _read_original(db, segment_id):
    try:
        payload = read_segment(db, segment_id)
        return {frame.commit_no: frame for frame in restore_segment_commits(payload)}
    except (SegmentUnavailable, ValueError) as exc:
        raise HistoryUnavailable(f'original history in segment {segment_id} is unavailable') from exc


def _image(row):
    if row is None:
        return None
    return [row.partition, format(row.reading, 'x')]


def _decode_image(row_id, image):
    if image is None:
        return None
    partition, reading = image
    if not isinstance(partition, str) or not isinstance(reading, str):
        raise ValueError('invalid archived row image')
    return Row(row_id, partition, int(reading, 16))


def _decode_archive(commit_no, payload):
    records = json.loads(payload)
    if not isinstance(records, list):
        raise ValueError('invalid archived commit frame')
    changes = []
    previous = None
    for row_id, old, new in records:
        if not isinstance(row_id, str) or (previous is not None and previous >= row_id):
            raise ValueError('invalid archived row order')
        before, after = _decode_image(row_id, old), _decode_image(row_id, new)
        if before == after:
            raise ValueError('archived frame contains an unchanged row')
        changes.append(RowEvidence(row_id, before, after))
        previous = row_id
    return CommitEvidence(commit_no, tuple(changes))


def read_commit_evidence(db, commit_no: int) -> CommitEvidence:
    located = db.execute(
        '''SELECT h.source_segment, a.changes FROM commit_history AS h
        LEFT JOIN history_archive AS a ON a.commit_no = h.commit_no
        WHERE h.commit_no = ?''', (commit_no,),
    ).fetchone()
    if located is not None:
        source_segment, archived = located
        if source_segment is not None:
            frame = _read_original(db, source_segment).get(commit_no)
            if frame is not None:
                return frame
        elif archived is not None:
            try:
                return _decode_archive(commit_no, archived)
            except (TypeError, ValueError) as exc:
                raise HistoryUnavailable(f'archived evidence for commit {commit_no} is invalid') from exc
    raise HistoryUnavailable(f'no readable evidence for commit {commit_no}')


def read_segment_evidence(db, segment_id: int, floor: int, head: int) -> tuple[CommitEvidence, ...]:
    expected = tuple(row[0] for row in db.execute(
        '''SELECT commit_no FROM commit_history
        WHERE source_segment = ? AND commit_no > ? AND commit_no <= ? ORDER BY commit_no''',
        (segment_id, floor, head),
    ))
    if not expected:
        return ()
    available = _read_original(db, segment_id)
    try:
        return tuple(available[commit_no] for commit_no in expected)
    except KeyError as exc:
        raise HistoryUnavailable(f'segment {segment_id} does not contain its indexed commits') from exc


def archive_evidence(db, segment_id: int, frame: CommitEvidence) -> None:
    updated = db.execute(
        'UPDATE commit_history SET source_segment = NULL WHERE commit_no = ? AND source_segment = ?',
        (frame.commit_no, segment_id),
    )
    if updated.rowcount != 1:
        raise HistoryUnavailable(f'evidence source changed for commit {frame.commit_no}')
    records = [[item.row_id, _image(item.before), _image(item.after)] for item in frame.changes]
    db.execute('INSERT INTO history_archive VALUES (?, ?)', (
        frame.commit_no, json.dumps(records, ensure_ascii=False, separators=(',', ':')),
    ))


def history_segment_dependencies(db) -> dict[int, tuple[int, ...]]:
    floor, head = read_history_bounds(db)
    grouped = {}
    for segment_id, commit_no in db.execute(
        '''SELECT source_segment, commit_no FROM commit_history
        WHERE source_segment IS NOT NULL AND commit_no > ? AND commit_no <= ?
        ORDER BY source_segment, commit_no''', (floor, head),
    ):
        grouped.setdefault(segment_id, []).append(commit_no)
    return {source: tuple(commits) for source, commits in grouped.items()}
