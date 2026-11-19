from lake.compact.publish import compact
from lake.compact.retire import retire
from lake.feed.changes import Change
from lake.feed.delivery import FeedPage, read_page
from lake.feed.subscription import open_feed
from lake.history.coverage import CoverageError, CoverageWindow, check_coverage
from lake.history.records import CommitEvidence
from lake.history.store import read_commit_evidence
from lake.ingest.batch import Edit
from lake.table.catalog import load_catalog
from lake.table.rows import Row
from tests.support import LakeCase, opening, pages, post_late, post_moves, submit


class FeedTests(LakeCase):
    def test_move_out_and_back(self):
        receipts = post_moves(self.db)
        self.assertEqual(tuple(receipt.commit_no for receipt in receipts), (40, 41, 42))
        east = open_feed(self.db, '东区', 40, 42, 10)
        west = open_feed(self.db, '西区', 40, 42, 10)
        east_row, west_row = Row('r', '东区', 5), Row('r', '西区', 7)
        self.assertEqual(read_page(self.db, east.subscription_id, east.first_cursor), FeedPage((
            Change(41, 'r', 'remove', east_row, None), Change(42, 'r', 'add', None, east_row),
        ), None))
        self.assertEqual(read_page(self.db, west.subscription_id, west.first_cursor), FeedPage((
            Change(41, 'r', 'add', None, west_row), Change(42, 'r', 'remove', west_row, None),
        ), None))
        north = open_feed(self.db, '北区', 40, 42, 10)
        self.assertEqual(read_page(self.db, north.subscription_id, north.first_cursor), FeedPage((), None))

    def test_move_inside_one_batch(self):
        opening(self.db)
        result = submit(self.db, 'within', Edit('update', 'r', '西区', 7), Edit('update', 'r', '东区', 5))
        self.assertEqual(result.commit_no, 41)
        sub = open_feed(self.db, '东区', 40, 41, 1)
        self.assertEqual(read_page(self.db, sub.subscription_id, sub.first_cursor), FeedPage((), None))

    def test_complete_empty_commit(self):
        opening(self.db)
        receipt = submit(self.db, 'within', Edit('update', 'r', '西区', 7), Edit('update', 'r', '东区', 5))
        self.assertEqual(submit(self.db, 'empty').commit_no, 42)
        sub = open_feed(self.db, '东区', 40, 42, 1)
        self.assertEqual(check_coverage(self.db, 40, 42), CoverageWindow(39, 42))
        self.assertEqual(read_commit_evidence(self.db, receipt.commit_no), CommitEvidence(41, ()))
        original_ids = load_catalog(self.db).segment_ids
        published = compact(self.db, original_ids)
        self.assertTrue(all(item.status == 'retired' for item in retire(self.db, published.replacement_id)))
        self.assertEqual(read_commit_evidence(self.db, 41), CommitEvidence(41, ()))
        self.assertEqual(read_commit_evidence(self.db, 42), CommitEvidence(42, ()))
        self.assertEqual(read_page(self.db, sub.subscription_id, sub.first_cursor), FeedPage((), None))
        same_cut = open_feed(self.db, '东区', 42, 42, 1)
        self.assertEqual(read_page(self.db, same_cut.subscription_id, same_cut.first_cursor), FeedPage((), None))

    def test_stay_update(self):
        opening(self.db)
        submit(self.db, 'stay', Edit('update', 'r', '东区', -2), Edit('update', 'r', '东区', 0))
        sub = open_feed(self.db, '东区', 40, 41, 1)
        page = read_page(self.db, sub.subscription_id, sub.first_cursor)
        self.assertEqual(page, FeedPage((Change(41, 'r', 'update', Row('r', '东区', 5), Row('r', '东区', 0)),), None))

    def test_stable_row_order(self):
        submit(self.db, 'reverse', Edit('insert', 'z', '东区', 9), Edit('insert', 'a', '东区', 1))
        submit(self.db, 'later', Edit('update', 'a', '东区', 2))
        sub = open_feed(self.db, '东区', 39, 41, 1)
        all_pages = pages(self.db, sub)
        self.assertEqual(tuple(page.changes for page in all_pages), (
            (Change(40, 'a', 'add', None, Row('a', '东区', 1)),),
            (Change(40, 'z', 'add', None, Row('z', '东区', 9)),),
            (Change(41, 'a', 'update', Row('a', '东区', 1), Row('a', '东区', 2)),),
        ))
        self.assertEqual(tuple(page.done for page in all_pages), (False, False, True))

    def test_uncovered_tail(self):
        post_moves(self.db)
        good = open_feed(self.db, '东区', 40, 42, 1)
        before = pages(self.db, good)
        self.assertEqual([item.commit_no for page in before for item in page.changes], [41, 42])
        with self.assertRaisesRegex(CoverageError, 'beyond committed head 42'):
            open_feed(self.db, '东区', 40, 43, 1)
        self.assertEqual(pages(self.db, good), before)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM subscriptions').fetchone(), (1,))
        self.assertEqual(post_late(self.db).commit_no, 43)
        extended = open_feed(self.db, '东区', 40, 43, 10)
        page = read_page(self.db, extended.subscription_id, extended.first_cursor)
        self.assertEqual([item.commit_no for item in page.changes], [41, 42, 43])
        with self.assertRaises(ValueError):
            open_feed(self.db, '东区', 42, 41, 1)
        with self.assertRaises(ValueError):
            open_feed(self.db, '东区', 40, 42, 0)

    def test_missing_evidence_is_not_empty(self):
        opening(self.db)
        submit(self.db, 'empty')
        sub = open_feed(self.db, '东区', 40, 41, 1)
        self.assertEqual(read_page(self.db, sub.subscription_id, sub.first_cursor), FeedPage((), None))
        self.db.execute('DELETE FROM commit_history WHERE commit_no = 41')
        with self.assertRaisesRegex(CoverageError, 'lacks evidence for commit 41'):
            read_page(self.db, sub.subscription_id, sub.first_cursor)
        with self.assertRaisesRegex(CoverageError, 'lacks evidence for commit 41'):
            open_feed(self.db, '东区', 40, 41, 1)
