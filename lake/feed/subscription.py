from dataclasses import dataclass

from lake.feed.cursor import encode_cursor, initial_cursor
from lake.history.coverage import check_coverage
from lake.ingest.validate import validate_text
from lake.table.storage import MAX_SQLITE_INTEGER, write_transaction


class SubscriptionUnavailable(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class Subscription:
    subscription_id: int
    partition: str
    start: int
    end: int
    page_size: int

    @property
    def first_cursor(self) -> str:
        return encode_cursor(initial_cursor(self.subscription_id))


def open_feed(db, partition: str, start: int, end: int, page_size: int) -> Subscription:
    validate_text(partition, 'partition')
    if type(page_size) is not int or page_size <= 0:
        raise ValueError('page_size must be a positive integer')
    with write_transaction(db):
        check_coverage(db, start, end)
        inserted = db.execute(
            '''INSERT INTO subscriptions (partition_name, start_commit, end_commit, page_size)
            VALUES (?, ?, ?, ?)''', (partition, start, end, format(page_size, 'x')),
        )
        return Subscription(inserted.lastrowid, partition, start, end, page_size)


def load_subscription(db, subscription_id: int) -> Subscription:
    if type(subscription_id) is not int or not 1 <= subscription_id <= MAX_SQLITE_INTEGER:
        raise SubscriptionUnavailable('expected a registered subscription ID')
    row = db.execute(
        '''SELECT partition_name, start_commit, end_commit, page_size FROM subscriptions
        WHERE subscription_id = ?''', (subscription_id,),
    ).fetchone()
    if row is None:
        raise SubscriptionUnavailable(f'unknown subscription {subscription_id}')
    partition, start, end, page_size = row
    return Subscription(subscription_id, partition, start, end, int(page_size, 16))
