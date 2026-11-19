from dataclasses import dataclass
import json

from lake.ingest.validate import validate_text


class InvalidCursor(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PageCursor:
    subscription_id: int
    after: tuple[int, str] | None = None


def initial_cursor(subscription_id: int) -> PageCursor:
    return PageCursor(subscription_id)


def encode_cursor(cursor: PageCursor) -> str:
    return json.dumps([1, cursor.subscription_id, cursor.after], ensure_ascii=False, separators=(',', ':'))


def decode_cursor(token: str, subscription_id: int) -> PageCursor:
    if not isinstance(token, str):
        raise InvalidCursor('page cursor must be text')
    try:
        parts = json.loads(token)
    except (ValueError, RecursionError) as exc:
        raise InvalidCursor('invalid page cursor encoding') from exc
    if not isinstance(parts, list) or len(parts) != 3:
        raise InvalidCursor('invalid page cursor shape')
    version, owner, after = parts
    if type(version) is not int or version != 1 or type(owner) is not int or owner <= 0:
        raise InvalidCursor('invalid page cursor version or subscription ID')
    if owner != subscription_id:
        raise InvalidCursor('page cursor belongs to another subscription')
    if after is None:
        return initial_cursor(owner)
    if not isinstance(after, list) or len(after) != 2 or type(after[0]) is not int or after[0] < 0:
        raise InvalidCursor('invalid page cursor position')
    try:
        row_id = validate_text(after[1], 'cursor row_id')
    except ValueError as exc:
        raise InvalidCursor('invalid page cursor row ID') from exc
    return PageCursor(owner, (after[0], row_id))
