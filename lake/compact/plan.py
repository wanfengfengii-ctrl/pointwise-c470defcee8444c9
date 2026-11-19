from dataclasses import dataclass
from typing import Iterable

from lake.table.catalog import CatalogView, StalePlan, load_catalog
from lake.table.storage import MAX_SQLITE_INTEGER, read_transaction


@dataclass(frozen=True, slots=True)
class CompactionPlan:
    generation: int
    segment_ids: tuple[int, ...]


def _check_selection(segment_ids):
    if not segment_ids:
        raise ValueError('select at least one active segment')
    if any(type(sid) is not int or not 1 <= sid <= MAX_SQLITE_INTEGER for sid in segment_ids):
        raise ValueError('segment IDs must be positive SQLite integers')
    if len(set(segment_ids)) != len(segment_ids):
        raise ValueError('a segment may only be selected once')


def plan_compaction(db, segment_ids: Iterable[int]) -> CompactionPlan:
    selected = tuple(segment_ids)
    _check_selection(selected)
    with read_transaction(db):
        catalog = load_catalog(db)
        if not set(selected).issubset(catalog.segment_ids):
            raise ValueError('a selected segment is not active')
        return CompactionPlan(catalog.generation, tuple(sorted(selected)))


def validate_plan(db, plan: CompactionPlan) -> CatalogView:
    _check_selection(plan.segment_ids)
    catalog = load_catalog(db)
    if catalog.generation != plan.generation or not set(plan.segment_ids).issubset(catalog.segment_ids):
        raise StalePlan('compaction plan no longer matches the current catalog')
    return catalog
