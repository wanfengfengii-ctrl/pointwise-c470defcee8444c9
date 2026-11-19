import sqlite3

from lake.ingest.admission import BatchConflict, apply
from lake.ingest.batch import BatchRequest, Edit
from lake.ingest.receipts import load_receipt
from lake.ingest.validate import InvalidBatch
from lake.table.catalog import load_catalog
from lake.table.rows import Row, read_current_rows
from lake.table.snapshot import SnapshotUnavailable, snapshot
from lake.table.storage import initialize_database
from tests.support import LakeCase, opening, submit


class BatchTests(LakeCase):
    def test_exact_retry(self):
        request = BatchRequest('opening', (Edit('insert', 'r', '东区', 5),))
        first = apply(self.db, request)
        self.assertEqual(first.commit_no, 40)
        self.assertEqual(apply(self.db, request), first)
        current = snapshot(self.db)
        self.assertEqual((current.commit_no, current.rows), (40, (Row('r', '东区', 5),)))
        self.assertEqual(len(load_catalog(self.db).segment_ids), 1)

    def test_conflicting_retry(self):
        receipt = opening(self.db)
        with self.assertRaises(BatchConflict):
            submit(self.db, 'opening', Edit('insert', 'r', '东区', 6))
        self.assertEqual(opening(self.db), receipt)
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 5),))
        self.assertEqual(load_catalog(self.db).head, 40)

    def test_ordered_edits(self):
        opening(self.db)
        six = Edit('update', 'r', '东区', 6)
        seven = Edit('update', 'r', '东区', 7)
        receipt = submit(self.db, 'ordered', six, seven)
        self.assertEqual(receipt.commit_no, 41)
        with self.assertRaises(BatchConflict):
            submit(self.db, 'ordered', seven, six)
        self.assertEqual(submit(self.db, 'ordered', six, seven), receipt)
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 7),))
        self.assertEqual(load_catalog(self.db).head, 41)

    def test_invalid_suffix_retry(self):
        opening(self.db)
        before = load_catalog(self.db)
        correction = Edit('update', 'r', '东区', 7)
        with self.assertRaises(InvalidBatch):
            submit(self.db, 'corrected', correction, Edit('delete', 'absent'))
        self.assertEqual(load_catalog(self.db), before)
        self.assertEqual(read_current_rows(self.db), (Row('r', '东区', 5),))
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 5),))
        self.assertIsNone(load_receipt(self.db, 'corrected'))
        receipt = submit(self.db, 'corrected', correction)
        self.assertEqual(receipt.commit_no, 41)
        self.assertEqual(submit(self.db, 'after', Edit('delete', 'r')).commit_no, 42)
        self.assertEqual(submit(self.db, 'corrected', correction), receipt)
        self.assertEqual(snapshot(self.db).rows, ())

    def test_initialization_repeated(self):
        receipt = opening(self.db)
        catalog = load_catalog(self.db)
        initialize_database(self.db)
        initialize_database(self.db, first_commit=40, retention=8)
        with self.assertRaisesRegex(ValueError, 'retention differs'):
            initialize_database(self.db, retention=9)
        self.reopen()
        self.assertEqual(load_catalog(self.db), catalog)
        self.assertEqual(opening(self.db), receipt)
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 5),))
        update = submit(self.db, 'next', Edit('update', 'r', '东区', 0))
        self.assertEqual(update.commit_no, 41)
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 0),))

    def test_invalid_fields_and_exact_integers(self):
        for edit in (Edit('insert', 'r', '东区', True), Edit('insert', 'r', '', 5),
                     Edit('insert', ' ', '东区', 5), Edit('delete', 'r', '东区'),
                     Edit('unknown', 'r', '东区', 5)):
            with self.subTest(edit=edit), self.assertRaises(InvalidBatch):
                submit(self.db, 'bad', edit)
        self.assertEqual(snapshot(self.db).commit_no, 39)
        self.assertIsNone(load_receipt(self.db, 'bad'))
        large = -(1 << 18000)
        submit(self.db, 'exact', Edit('insert', 'r', '东区', large), Edit('insert', 's', '东区', 0))
        self.reopen()
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', large), Row('s', '东区', 0)))
        with self.assertRaises(SnapshotUnavailable):
            snapshot(self.db, 'unknown')

    def test_receipt_write_failure_rolls_back_commit(self):
        receipt = opening(self.db)
        original = load_catalog(self.db)
        self.db.execute('''CREATE TEMP TRIGGER reject_receipt BEFORE INSERT ON main.receipts
            BEGIN SELECT RAISE(ABORT, 'receipt write rejected'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt write rejected'):
            submit(self.db, 'blocked', Edit('update', 'r', '西区', 7))
        self.assertEqual(load_catalog(self.db), original)
        self.assertEqual(read_current_rows(self.db), (Row('r', '东区', 5),))
        self.assertIsNone(load_receipt(self.db, 'blocked'))
        self.assertEqual(opening(self.db), receipt)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM commit_history').fetchone(), (1,))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM segments').fetchone(), (1,))
