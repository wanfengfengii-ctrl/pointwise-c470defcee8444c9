from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from lake.feed.delivery import read_page
from lake.ingest.admission import apply
from lake.ingest.batch import BatchRequest, Edit
from lake.table.snapshot import register_snapshot
from lake.table.storage import open_database


class LakeCase(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.path = self.directory / 'main.sqlite'
        self.db = self.make_database(self.path)

    def make_database(self, path, *, retention=8):
        db = open_database(path, first_commit=40, retention=retention)
        self.addCleanup(db.close)
        return db

    def reopen(self):
        self.db.close()
        self.db = open_database(self.path)
        self.addCleanup(self.db.close)
        return self.db


def submit(db, batch_id, *edits):
    return apply(db, BatchRequest(batch_id, edits))


def opening(db):
    return submit(db, 'opening', Edit('insert', 'r', '东区', 5))


def post_moves(db):
    start = opening(db)
    register_snapshot(db, 'shift-40')
    transfer = submit(db, 'transfer', Edit('update', 'r', '西区', 7))
    returned = submit(db, 'return', Edit('update', 'r', '东区', 5))
    register_snapshot(db, 'shift-42')
    return start, transfer, returned


def post_late(db):
    return submit(db, 'late', Edit('update', 'r', '东区', 9))


def pages(db, subscription):
    token = subscription.first_cursor
    result = []
    while token is not None:
        page = read_page(db, subscription.subscription_id, token)
        result.append(page)
        token = page.next_cursor
    return tuple(result)
