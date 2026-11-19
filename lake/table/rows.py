from dataclasses import dataclass
from typing import Iterable, Mapping

from lake.ingest.batch import Edit
from lake.ingest.validate import InvalidBatch


@dataclass(frozen=True, slots=True)
class Row:
    row_id: str
    partition: str
    reading: int


@dataclass(frozen=True, slots=True)
class RowSettlement:
    before: dict[str, Row | None]
    after: dict[str, Row | None]

    def net_changes(self):
        for row_id in sorted(self.before):
            before = self.before[row_id]
            after = self.after[row_id]
            if before != after:
                yield row_id, before, after


def settle_rows(start: Mapping[str, Row], edits: Iterable[Edit]) -> RowSettlement:
    before = {}
    after = {}
    for edit in edits:
        row_id = edit.row_id
        if row_id not in before:
            before[row_id] = after[row_id] = start.get(row_id)
        existing = after[row_id]
        if edit.operation == 'insert':
            if existing is not None:
                raise InvalidBatch(f'insert requires an absent row: {row_id!r}')
        elif edit.operation in ('update', 'delete'):
            if existing is None:
                raise InvalidBatch(f'{edit.operation} requires a present row: {row_id!r}')
        else:
            raise InvalidBatch(f'unknown edit operation: {edit.operation!r}')
        after[row_id] = (
            None if edit.operation == 'delete'
            else Row(row_id, edit.partition, edit.reading)
        )
    return RowSettlement(before, after)


def read_current_rows(db, row_ids: Iterable[str] | None = None) -> tuple[Row, ...]:
    columns = 'SELECT row_id, partition_name, reading FROM current_rows'
    if row_ids is None:
        records = db.execute(columns + ' ORDER BY row_id')
        return tuple(Row(key, partition, int(reading, 16)) for key, partition, reading in records)
    rows = []
    for row_id in sorted(set(row_ids)):
        record = db.execute(columns + ' WHERE row_id = ?', (row_id,)).fetchone()
        if record is not None:
            key, partition, reading = record
            rows.append(Row(key, partition, int(reading, 16)))
    return tuple(rows)


def write_settlement(db, settlement: RowSettlement) -> None:
    for row_id, _, after in settlement.net_changes():
        if after is None:
            db.execute('DELETE FROM current_rows WHERE row_id = ?', (row_id,))
        else:
            db.execute(
                '''INSERT INTO current_rows (row_id, partition_name, reading) VALUES (?, ?, ?)
                ON CONFLICT (row_id) DO UPDATE SET
                    partition_name = excluded.partition_name, reading = excluded.reading''',
                (row_id, after.partition, format(after.reading, 'x')),
            )
