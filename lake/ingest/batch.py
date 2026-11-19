from dataclasses import dataclass
import json


@dataclass(frozen=True, slots=True)
class Edit:
    operation: str
    row_id: str
    partition: str | None = None
    reading: int | None = None


@dataclass(frozen=True, slots=True)
class BatchRequest:
    batch_id: str
    edits: tuple[Edit, ...]


def request_identity(request: BatchRequest) -> str:
    """Serialize the validated ordered request for exact equality on retry."""
    edits = [
        [edit.operation, edit.row_id, edit.partition,
         None if edit.reading is None else format(edit.reading, 'x')]
        for edit in request.edits
    ]
    return json.dumps([request.batch_id, edits], ensure_ascii=False, separators=(',', ':'))
