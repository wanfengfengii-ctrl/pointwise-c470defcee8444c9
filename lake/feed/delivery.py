from dataclasses import dataclass

from lake.feed.changes import Change, project_partition
from lake.feed.cursor import decode_cursor, encode_cursor
from lake.feed.pages import slice_changes
from lake.feed.subscription import load_subscription
from lake.history.coverage import read_covered_interval
from lake.table.storage import read_transaction


@dataclass(frozen=True, slots=True)
class FeedPage:
    changes: tuple[Change, ...]
    next_cursor: str | None

    @property
    def done(self) -> bool:
        return self.next_cursor is None


def read_page(db, subscription_id: int, token: str) -> FeedPage:
    with read_transaction(db):
        subscription = load_subscription(db, subscription_id)
        cursor = decode_cursor(token, subscription.subscription_id)
        frames = read_covered_interval(db, subscription.start, subscription.end)
        projected = (change for frame in frames for change in project_partition(frame, subscription.partition))
        page = slice_changes(projected, cursor, subscription.page_size)
        next_token = None if page.next_cursor is None else encode_cursor(page.next_cursor)
        return FeedPage(page.changes, next_token)
