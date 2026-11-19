from dataclasses import dataclass
from typing import Iterable

from lake.compact.eligibility import assess_retirement
from lake.compact.plan import plan_compaction, validate_plan
from lake.compact.rewrite import PreparedReplacement, prepare_replacement
from lake.history.store import HistoryUnavailable, archive_evidence, history_segment_dependencies
from lake.table.catalog import replace_segments
from lake.table.rows import read_current_rows
from lake.table.segments import write_segment
from lake.table.snapshot import rebind_snapshot_sources, visible_rows
from lake.table.storage import MAX_SQLITE_INTEGER, write_transaction


class ReplacementUnavailable(LookupError):
    pass


class InvalidReplacement(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PublishedReplacement:
    replacement_id: int
    generation: int
    new_segment_id: int
    original_ids: tuple[int, ...]
    eligible_ids: tuple[int, ...]


def _transfer_history(db, prepared):
    dependencies = history_segment_dependencies(db)
    required = {
        (segment_id, commit_no)
        for segment_id in prepared.plan.segment_ids
        for commit_no in dependencies.get(segment_id, ())
    }
    present = {(segment_id, frame.commit_no) for segment_id, frame in prepared.evidence}
    if present != required or len(present) != len(prepared.evidence):
        raise HistoryUnavailable('prepared history does not cover the selected original segments')
    for segment_id, frame in prepared.evidence:
        archive_evidence(db, segment_id, frame)


def publish_replacement(db, prepared: PreparedReplacement) -> PublishedReplacement:
    with write_transaction(db):
        plan = prepared.plan
        catalog = validate_plan(db, plan)
        new_id = write_segment(db, prepared.payload)
        candidate = (set(catalog.segment_ids) - set(plan.segment_ids)) | {new_id}
        if visible_rows(db, candidate, catalog.head) != read_current_rows(db):
            raise InvalidReplacement('the replacement does not reproduce current rows')
        _transfer_history(db, prepared)
        rebind_snapshot_sources(db, plan.segment_ids, new_id)
        generation = replace_segments(db, plan.generation, plan.segment_ids, new_id)
        eligibility = assess_retirement(db, plan.segment_ids)
        inserted = db.execute(
            'INSERT INTO replacements (generation, new_segment) VALUES (?, ?)', (generation, new_id)
        )
        replacement_id = inserted.lastrowid
        db.executemany(
            '''INSERT INTO replacement_inputs (replacement_id, segment_id, eligible_at_publish)
            VALUES (?, ?, ?)''',
            ((replacement_id, item.segment_id, int(item.eligible)) for item in eligibility),
        )
        return load_replacement(db, replacement_id)


def load_replacement(db, replacement_id: int) -> PublishedReplacement:
    if type(replacement_id) is not int or not 1 <= replacement_id <= MAX_SQLITE_INTEGER:
        raise ReplacementUnavailable('expected a published replacement ID')
    row = db.execute(
        'SELECT generation, new_segment FROM replacements WHERE replacement_id = ?', (replacement_id,)
    ).fetchone()
    if row is None:
        raise ReplacementUnavailable(f'replacement {replacement_id} has not been published')
    originals = db.execute(
        '''SELECT segment_id, eligible_at_publish FROM replacement_inputs
        WHERE replacement_id = ? ORDER BY segment_id''', (replacement_id,),
    ).fetchall()
    return PublishedReplacement(
        replacement_id, row[0], row[1],
        tuple(segment_id for segment_id, _ in originals),
        tuple(segment_id for segment_id, eligible in originals if eligible),
    )


def compact(db, segment_ids: Iterable[int]) -> PublishedReplacement:
    plan = plan_compaction(db, segment_ids)
    prepared = prepare_replacement(db, plan)
    return publish_replacement(db, prepared)
