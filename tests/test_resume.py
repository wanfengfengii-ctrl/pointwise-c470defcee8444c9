from lake.compact.publish import compact
from lake.compact.retire import retire
from lake.feed.changes import Change
from lake.feed.cursor import InvalidCursor, PageCursor, encode_cursor
from lake.feed.delivery import FeedPage, read_page
from lake.feed.subscription import open_feed
from lake.history.coverage import ExpiredWindow, read_covered_interval
from lake.history.records import CommitEvidence, RowEvidence
from lake.table.catalog import load_catalog
from lake.table.rows import Row
from lake.table.segments import SegmentUnavailable, read_segment
from lake.table.snapshot import snapshot
from tests.support import LakeCase, pages, post_late, post_moves


class ResumeTests(LakeCase):
    def test_resume_after_retirement(self):
        post_moves(self.db)
        sub = open_feed(self.db, '东区', 40, 42, 1)
        first = read_page(self.db, sub.subscription_id, sub.first_cursor)
        self.assertEqual(first.changes, (Change(41, 'r', 'remove', Row('r', '东区', 5), None),))
        self.assertFalse(first.done)
        originals = load_catalog(self.db).segment_ids[1:]
        result = compact(self.db, originals)
        retired = retire(self.db, result.replacement_id)
        self.assertEqual(tuple((item.segment_id, item.status) for item in retired),
                         tuple((sid, 'retired') for sid in originals))
        self.reopen()
        self.assertEqual(post_late(self.db).commit_no, 43)
        for sid in originals:
            with self.assertRaises(SegmentUnavailable):
                read_segment(self.db, sid)
        second = read_page(self.db, sub.subscription_id, first.next_cursor)
        self.assertEqual(second, FeedPage((Change(42, 'r', 'add', None, Row('r', '东区', 5)),), None))
        self.assertEqual(read_page(self.db, sub.subscription_id, first.next_cursor), second)
        self.assertEqual(read_page(self.db, sub.subscription_id, sub.first_cursor), first)

    def test_history_handoff(self):
        control = self.make_database(self.directory / 'control.sqlite')
        post_moves(control)
        post_moves(self.db)
        baseline = open_feed(control, '东区', 40, 42, 1)
        sub = open_feed(self.db, '东区', 40, 42, 1)
        east, west = Row('r', '东区', 5), Row('r', '西区', 7)
        expected = (CommitEvidence(41, (RowEvidence('r', east, west),)),
                    CommitEvidence(42, (RowEvidence('r', west, east),)))
        self.assertEqual(read_covered_interval(control, 40, 42), expected)
        self.assertEqual(read_covered_interval(self.db, 40, 42), expected)
        originals = load_catalog(self.db).segment_ids[1:]
        for sid in originals:
            self.assertEqual(len(read_segment(self.db, sid).edits), 1)
        published = compact(self.db, originals)
        self.assertEqual(published.eligible_ids, originals)
        self.assertTrue(all(item.status == 'retired' for item in retire(self.db, published.replacement_id)))
        self.reopen()
        for sid in originals:
            with self.assertRaises(SegmentUnavailable):
                read_segment(self.db, sid)
            for table in ('segment_mutations', 'segment_base', 'segment_edits'):
                self.assertEqual(self.db.execute(f'SELECT COUNT(*) FROM {table} WHERE segment_id = ?', (sid,)).fetchone(), (0,))
        self.assertEqual(self.db.execute(
            'SELECT commit_no, source_segment FROM commit_history WHERE commit_no > 40 ORDER BY commit_no'
        ).fetchall(), [(41, None), (42, None)])
        self.assertEqual(read_covered_interval(self.db, 40, 42), expected)
        post_late(control)
        post_late(self.db)
        self.assertEqual(pages(self.db, sub), pages(control, baseline))
        self.assertEqual(snapshot(self.db, 'shift-40'), snapshot(control, 'shift-40'))
        self.assertEqual(snapshot(self.db, 'shift-42'), snapshot(control, 'shift-42'))
        self.assertEqual(snapshot(self.db), snapshot(control))

    def test_fixed_end(self):
        post_moves(self.db)
        sub = open_feed(self.db, '东区', 40, 42, 1)
        token = read_page(self.db, sub.subscription_id, sub.first_cursor).next_cursor
        self.assertIsNotNone(token)
        self.reopen()
        post_late(self.db)
        self.assertEqual((snapshot(self.db).commit_no, snapshot(self.db).rows), (43, (Row('r', '东区', 9),)))
        expected = FeedPage((Change(42, 'r', 'add', None, Row('r', '东区', 5)),), None)
        self.assertEqual(read_page(self.db, sub.subscription_id, token), expected)
        self.assertEqual(read_page(self.db, sub.subscription_id, token), expected)
        late_sub = open_feed(self.db, '东区', 42, 43, 1)
        self.assertEqual(read_page(self.db, late_sub.subscription_id, late_sub.first_cursor),
                         FeedPage((Change(43, 'r', 'update', Row('r', '东区', 5), Row('r', '东区', 9)),), None))

    def test_expired_window(self):
        db = self.make_database(self.directory / 'tight.sqlite', retention=2)
        post_moves(db)
        sub = open_feed(db, '东区', 40, 42, 1)
        token = read_page(db, sub.subscription_id, sub.first_cursor).next_cursor
        replacement = compact(db, load_catalog(db).segment_ids[1:])
        retire(db, replacement.replacement_id)
        self.assertTrue(read_page(db, sub.subscription_id, token).done)
        post_late(db)
        with self.assertRaisesRegex(ExpiredWindow, 'below retained start 41'):
            read_page(db, sub.subscription_id, token)
        fresh = open_feed(db, '东区', 41, 43, 2)
        self.assertEqual([item.commit_no for item in read_page(db, fresh.subscription_id, fresh.first_cursor).changes], [42, 43])

    def test_foreign_cursor(self):
        post_moves(self.db)
        east = open_feed(self.db, '东区', 40, 42, 1)
        west = open_feed(self.db, '西区', 40, 42, 1)
        east_token = read_page(self.db, east.subscription_id, east.first_cursor).next_cursor
        west_token = read_page(self.db, west.subscription_id, west.first_cursor).next_cursor
        self.assertIsNotNone(west_token)
        with self.assertRaisesRegex(InvalidCursor, 'another subscription'):
            read_page(self.db, east.subscription_id, west_token)
        self.assertEqual(read_page(self.db, east.subscription_id, east_token).changes,
                         (Change(42, 'r', 'add', None, Row('r', '东区', 5)),))

    def test_same_partition_other_subscription(self):
        post_moves(self.db)
        one = open_feed(self.db, '东区', 40, 42, 1)
        two = open_feed(self.db, '东区', 40, 42, 1)
        one_token = read_page(self.db, one.subscription_id, one.first_cursor).next_cursor
        two_token = read_page(self.db, two.subscription_id, two.first_cursor).next_cursor
        self.assertIsNotNone(one_token)
        self.assertIsNotNone(two_token)
        for subscription, foreign in ((one, two_token), (two, one_token)):
            with self.assertRaises(InvalidCursor):
                read_page(self.db, subscription.subscription_id, foreign)
        expected = FeedPage((Change(42, 'r', 'add', None, Row('r', '东区', 5)),), None)
        self.assertEqual(read_page(self.db, one.subscription_id, one_token), expected)
        self.assertEqual(read_page(self.db, two.subscription_id, two_token), expected)

    def test_bad_cursor_positions(self):
        post_moves(self.db)
        sub = open_feed(self.db, '东区', 40, 42, 1)
        for token in ('unparseable', encode_cursor(PageCursor(sub.subscription_id, (41, 'x'))),
                      encode_cursor(PageCursor(sub.subscription_id, (42, 'r')))):
            with self.subTest(token=token), self.assertRaises(InvalidCursor):
                read_page(self.db, sub.subscription_id, token)
        self.assertFalse(read_page(self.db, sub.subscription_id, sub.first_cursor).done)
