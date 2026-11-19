import sqlite3

from lake.compact.plan import plan_compaction
from lake.compact.publish import ReplacementUnavailable, compact, load_replacement, publish_replacement
from lake.compact.retire import RetirementResult, retire
from lake.compact.rewrite import prepare_replacement
from lake.feed.delivery import read_page
from lake.feed.subscription import open_feed
from lake.history.store import read_commit_evidence
from lake.ingest.batch import Edit
from lake.table.catalog import StalePlan, load_catalog
from lake.table.rows import Row
from lake.table.segments import SegmentMutation, SegmentUnavailable, read_segment
from lake.table.snapshot import register_snapshot, snapshot
from tests.support import LakeCase, opening, post_moves, submit


class CompactionTests(LakeCase):
    def test_retire_before_publish(self):
        post_moves(self.db)
        catalog = load_catalog(self.db)
        selected = catalog.segment_ids[1:]
        payloads = tuple(read_segment(self.db, sid) for sid in selected)
        prepared = prepare_replacement(self.db, plan_compaction(self.db, selected))
        self.assertEqual(prepared.plan.segment_ids, selected)
        with self.assertRaises(ReplacementUnavailable):
            retire(self.db, 1)
        self.assertEqual(load_catalog(self.db), catalog)
        self.assertEqual(tuple(read_segment(self.db, sid) for sid in selected), payloads)
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 5),))

    def test_snapshot_pin(self):
        opening(self.db)
        submit(self.db, 'transfer', Edit('update', 'r', '西区', 7))
        pinned = register_snapshot(self.db, 'shift-41')
        submit(self.db, 'return', Edit('update', 'r', '东区', 5))
        selected = load_catalog(self.db).segment_ids[1:]
        original, release = selected
        replacement = compact(self.db, selected)
        self.assertEqual(replacement.eligible_ids, (release,))
        retirement = retire(self.db, replacement.replacement_id)
        self.assertEqual(retirement, (RetirementResult(original, 'blocked', ('fixed_snapshot',)),
                                      RetirementResult(release, 'retired')))
        self.reopen()
        self.assertEqual(snapshot(self.db, 'shift-41'), pinned)
        self.assertEqual(snapshot(self.db).rows, (Row('r', '东区', 5),))
        self.assertEqual(read_segment(self.db, original).source_commit, 41)
        with self.assertRaises(SegmentUnavailable):
            read_segment(self.db, release)
        self.assertEqual(retire(self.db, replacement.replacement_id), (
            RetirementResult(original, 'blocked', ('fixed_snapshot',)), RetirementResult(release, 'already_retired'),
        ))

    def test_failed_swap_keeps_feed(self):
        post_moves(self.db)
        before = load_catalog(self.db)
        fixed = snapshot(self.db, 'shift-42')
        selected = before.segment_ids[1:]
        feed = open_feed(self.db, '东区', 40, 42, 1)
        token = read_page(self.db, feed.subscription_id, feed.first_cursor).next_cursor
        expected_second = read_page(self.db, feed.subscription_id, token)
        originals = tuple(read_segment(self.db, sid) for sid in selected)
        frame = read_commit_evidence(self.db, 41)
        prepared = prepare_replacement(self.db, plan_compaction(self.db, selected))
        self.db.execute('CREATE TEMP TABLE publish_gate (id INTEGER PRIMARY KEY)')
        self.db.execute('''CREATE TEMP TABLE publish_trip (
            id INTEGER REFERENCES publish_gate(id) DEFERRABLE INITIALLY DEFERRED)''')
        self.db.execute('''CREATE TEMP TRIGGER reject_swap BEFORE UPDATE OF generation ON main.lake_meta
            BEGIN INSERT INTO publish_trip VALUES (1); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'FOREIGN KEY constraint failed'):
            publish_replacement(self.db, prepared)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(load_catalog(self.db), before)
        self.assertEqual(snapshot(self.db).commit_no, 42)
        self.assertEqual(snapshot(self.db).rows, fixed.rows)
        self.assertEqual(snapshot(self.db, 'shift-42'), fixed)
        self.assertEqual(read_page(self.db, feed.subscription_id, token), expected_second)
        self.assertEqual(read_commit_evidence(self.db, 41), frame)
        self.assertEqual(tuple(read_segment(self.db, sid) for sid in selected), originals)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM history_archive').fetchone(), (0,))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM publish_trip').fetchone(), (0,))
        with self.assertRaises(ReplacementUnavailable):
            load_replacement(self.db, 1)
        self.db.execute('DROP TRIGGER reject_swap')
        result = publish_replacement(self.db, prepared)
        self.assertEqual(result.eligible_ids, selected)
        self.assertEqual(read_page(self.db, feed.subscription_id, token), expected_second)

    def test_stale_plan(self):
        post_moves(self.db)
        ids = load_catalog(self.db).segment_ids
        stale = prepare_replacement(self.db, plan_compaction(self.db, ids[:2]))
        sub = open_feed(self.db, '东区', 40, 42, 1)
        token = read_page(self.db, sub.subscription_id, sub.first_cursor).next_cursor
        second = read_page(self.db, sub.subscription_id, token)
        winner = compact(self.db, ids[1:])
        catalog = load_catalog(self.db)
        with self.assertRaises(StalePlan):
            publish_replacement(self.db, stale)
        self.assertEqual(load_catalog(self.db), catalog)
        self.assertEqual(catalog.segment_ids, (ids[0], winner.new_segment_id))
        self.assertEqual(load_replacement(self.db, winner.replacement_id), winner)
        self.assertEqual(snapshot(self.db, 'shift-42').rows, (Row('r', '东区', 5),))
        self.assertEqual(read_page(self.db, sub.subscription_id, token), second)

    def test_success_preserves_rows(self):
        submit(self.db, 'opening', Edit('insert', 'r', '东区', 5), Edit('insert', 'x', '东区', 2))
        submit(self.db, 'withdraw', Edit('delete', 'r'), Edit('update', 'x', '东区', 3))
        submit(self.db, 'unselected-newer', Edit('update', 'x', '东区', 4))
        submit(self.db, 'add', Edit('insert', 'y', '西区', 0))
        fixed = register_snapshot(self.db, 'shift-43')
        previous = load_catalog(self.db)
        selected = (previous.segment_ids[1], previous.segment_ids[3])
        result = compact(self.db, reversed(selected))
        catalog = load_catalog(self.db)
        self.assertEqual(catalog.head, 43)
        self.assertEqual(catalog.segment_ids, (previous.segment_ids[0], previous.segment_ids[2], result.new_segment_id))
        self.assertEqual(read_segment(self.db, result.new_segment_id).mutations, (
            SegmentMutation('r', 41, None), SegmentMutation('x', 41, Row('x', '东区', 3)),
            SegmentMutation('y', 43, Row('y', '西区', 0)),
        ))
        self.assertEqual(result.eligible_ids, selected)
        self.assertTrue(all(item.status == 'retired' for item in retire(self.db, result.replacement_id)))
        self.assertEqual(snapshot(self.db, 'shift-43'), fixed)
        self.assertEqual(snapshot(self.db).rows, (Row('x', '东区', 4), Row('y', '西区', 0)))
        following = compact(self.db, catalog.segment_ids)
        self.assertEqual(following.eligible_ids, catalog.segment_ids)
        self.assertTrue(all(item.status == 'retired' for item in retire(self.db, following.replacement_id)))
        self.assertEqual(snapshot(self.db, 'shift-43'), fixed)
        self.assertEqual(load_replacement(self.db, result.replacement_id), result)
        self.assertEqual(snapshot(self.db).rows, fixed.rows)
