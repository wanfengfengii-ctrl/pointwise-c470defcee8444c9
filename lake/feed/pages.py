from bisect import bisect_left
from dataclasses import dataclass
from typing import Iterable

from lake.feed.changes import Change
from lake.feed.cursor import InvalidCursor, PageCursor


@dataclass(frozen=True, slots=True)
class PageSlice:
    changes: tuple[Change, ...]
    next_cursor: PageCursor | None


def slice_changes(changes: Iterable[Change], cursor: PageCursor, page_size: int) -> PageSlice:
    ordered = sorted(changes, key=lambda change: change.position)
    start = 0
    if cursor.after is not None:
        keys = [change.position for change in ordered]
        index = bisect_left(keys, cursor.after)
        start = index + 1
        if start >= len(keys) or keys[index] != cursor.after or start % page_size:
            raise InvalidCursor('page cursor does not refer to an issued continuation boundary')
    stop = min(start + page_size, len(ordered))
    page = tuple(ordered[start:stop])
    next_cursor = PageCursor(cursor.subscription_id, page[-1].position) if stop < len(ordered) else None
    return PageSlice(page, next_cursor)
