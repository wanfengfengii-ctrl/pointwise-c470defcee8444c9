from dataclasses import dataclass


class StalePlan(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CatalogView:
    generation: int
    head: int
    segment_ids: tuple[int, ...]


def load_catalog(db) -> CatalogView:
    generation, head = db.execute(
        'SELECT generation, head FROM lake_meta WHERE singleton = 1'
    ).fetchone()
    ids = tuple(row[0] for row in db.execute('SELECT segment_id FROM active_segments ORDER BY segment_id'))
    return CatalogView(generation, head, ids)


def append_segment(db, segment_id: int) -> None:
    db.execute('INSERT INTO active_segments (segment_id) VALUES (?)', (segment_id,))
    db.execute('UPDATE lake_meta SET generation = generation + 1 WHERE singleton = 1')


def replace_segments(db, generation: int, original_ids: tuple[int, ...], new_id: int) -> int:
    current = load_catalog(db)
    if current.generation != generation or not set(original_ids).issubset(current.segment_ids):
        raise StalePlan('selected segments no longer match the current catalog')
    db.executemany('DELETE FROM active_segments WHERE segment_id = ?', ((sid,) for sid in original_ids))
    append_segment(db, new_id)
    return generation + 1
