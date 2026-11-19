"""Public source fixtures and offline-replica checks for REPLICA.md."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch

EAST = '东区'
WEST = '西区'


def require_equal(actual, expected):
    if actual != expected:
        raise AssertionError(f'expected {expected!r}, got {actual!r}')
    return actual


def expect_error(error, call, *args):
    try:
        call(*args)
    except error:
        return
    raise AssertionError(f'{call.__name__} succeeded, expected {error.__name__}')


def build_source(db, case):
    from lake.feed.subscription import open_feed
    from lake.ingest.admission import apply
    from lake.ingest.batch import BatchRequest, Edit
    from lake.table.snapshot import register_snapshot

    apply(db, BatchRequest('opening', (Edit('insert', 'a', EAST, 1),
                                      Edit('insert', 'b', EAST, 2),
                                      Edit('insert', 'w', WEST, 8))))
    register_snapshot(db, 'bottom')
    a = Edit('update', 'a', EAST, 5)
    b = Edit('update', 'b', EAST, 7)
    late = Edit('update', 'a', EAST, 9)
    batches = {'A': ((a, b), (), (late,)),
               'B': ((a,), (b,), (late,)),
               'empty': ((), (Edit('update', 'w', WEST, -2),), ())}
    for commit, edits in enumerate(batches[case], 41):
        receipt = apply(db, BatchRequest(f'batch-{commit}', edits))
        require_equal(receipt.commit_no, commit)
    register_snapshot(db, 'end')
    return open_feed(db, EAST, 40, 43, 1)


def probe():
    from lake.feed.changes import project_partition
    from lake.feed.delivery import read_page
    from lake.history.coverage import read_covered_interval
    from lake.table.storage import open_database

    first = []
    counts = {}
    for case in ('A', 'B', 'empty'):
        with closing(open_database(':memory:', first_commit=40)) as db:
            sub = build_source(db, case)
            page = read_page(db, sub.subscription_id, sub.first_cursor)
            counts[case] = [len(project_partition(frame, EAST))
                            for frame in read_covered_interval(db, 40, 43)]
            if case == 'empty':
                require_equal((page.changes, page.done), ((), True))
            else:
                require_equal([(c.commit_no, c.row_id, c.after.reading) for c in page.changes],
                              [(41, 'a', 5)])
                require_equal(page.done, False)
                first.append(page)
    require_equal(first[0], first[1])
    require_equal(counts, {'A': [2, 0, 1], 'B': [1, 1, 1], 'empty': [0, 0, 0]})
    print(json.dumps({'v1_same_first_page': True, 'projection_counts_41_to_43': counts}))


class CommitGate:
    """Inject SQLite refusal after writes, independently of replica table names."""

    def __init__(self):
        self.real_connect = sqlite3.connect
        self.connections = []
        self.armed = False
        self.denials = 0

    def connect(self, *args, **kwargs):
        conn = self.real_connect(*args, **kwargs)
        self.connections.append(conn)
        if self.armed:
            self.watch(conn)
        return conn

    def watch(self, conn):
        baseline = conn.total_changes

        def authorize(op, action, *_):
            boundary = ((op, action) == (sqlite3.SQLITE_TRANSACTION, 'COMMIT')
                        or (op, action) == (sqlite3.SQLITE_SAVEPOINT, 'RELEASE'))
            if self.armed and boundary and conn.total_changes > baseline:
                self.denials += 1
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(authorize)

    def arm(self):
        self.armed = True
        for conn in self.connections:
            try:
                self.watch(conn)
            except sqlite3.ProgrammingError:
                continue  # A source or an earlier replica has already closed.


def run_examples(directory, gate=None):
    from lake.feed.delivery import export_replica
    from lake.feed.replica import Replica
    from lake.feed.subscription import open_feed
    from lake.table.rows import Row
    from lake.table.snapshot import snapshot
    from lake.table.storage import open_database

    bundles = {}
    for case in ('A', 'B', 'empty'):
        with closing(open_database(':memory:', first_commit=40)) as db:
            sub = build_source(db, case)
            bundles[case] = export_replica(db, 'bottom', sub.subscription_id)
            if case == 'A':
                fixed_end = snapshot(db, 'end')
                end_rows = tuple(row for row in fixed_end.rows if row.partition == EAST)
                require_equal((fixed_end.commit_no, end_rows),
                              (43, (Row('a', EAST, 9), Row('b', EAST, 7))))
                other = open_feed(db, EAST, 40, 43, 1)
                foreign_page = export_replica(db, 'bottom', other.subscription_id)[2][0]
                expect_error(ValueError, export_replica, db, 'end', sub.subscription_id)

    # All source connections are now closed. Replicas use separate files.
    receipt, base, pages = bundles['A']
    require_equal(base, (Row('a', EAST, 1), Row('b', EAST, 2)))
    first, middle, last = pages
    target = directory / 'out-of-order.sqlite'
    with closing(Replica.open(target)) as replica:
        wrong_base = (Row('a', EAST, 99), base[1])
        expect_error(ValueError, replica.install, wrong_base, receipt)
        replica.install(base, receipt)
        expect_error(ValueError, replica.accept, foreign_page)
        damaged = bytes([first[0] ^ 1]) + first[1:]
        expect_error(ValueError, replica.accept, damaged)
        require_equal(replica.accept(last), 40)
        require_equal(replica.accept(first), 40)
        pending = require_equal(replica.read(), (40, base))
        if gate is not None:
            gate.arm()
            try:
                expect_error(sqlite3.DatabaseError, replica.accept, middle)
            finally:
                gate.armed = False
            if gate.denials == 0:
                raise AssertionError('no SQLite write commit reached the failure fixture')
            require_equal(replica.read(), pending)
    with closing(Replica.open(target)) as replica:
        require_equal(replica.read(), pending)
        require_equal(replica.accept(middle), 43)
        end = require_equal(replica.read(), (43, end_rows))
        require_equal(replica.accept(middle), 43)
        require_equal(replica.read(), end)

    with closing(Replica.open(directory / 'in-order.sqlite')) as replica:
        replica.install(base, receipt)
        require_equal(replica.accept(first), 40)
        require_equal(replica.accept(middle), 42)
        through_empty = require_equal(replica.read(), (42, (Row('a', EAST, 5), Row('b', EAST, 7))))
        require_equal(replica.accept(middle), 42)
        require_equal(replica.accept(last), 43)
        require_equal(replica.read(), end)

    other_receipt, other_base, other_pages = bundles['B']
    require_equal(other_base, base)
    require_equal(len(other_pages), 3)
    with closing(Replica.open(directory / 'same-v1-page.sqlite')) as replica:
        replica.install(other_base, other_receipt)
        require_equal(replica.accept(other_pages[0]), 41)
        other_first = require_equal(replica.read(), (41, (Row('a', EAST, 5), Row('b', EAST, 2))))

    empty_receipt, empty_base, (empty_page,) = bundles['empty']
    require_equal(empty_base, base)
    with closing(Replica.open(directory / 'empty.sqlite')) as replica:
        replica.install(empty_base, empty_receipt)
        require_equal(replica.read(), (40, base))
        require_equal(replica.accept(empty_page), 43)
        require_equal(replica.read(), (43, base))

    states = {'A_gap': pending, 'B_first': other_first,
              'A_empty_42': through_empty, 'A_reopened': end}
    return {key: [commit, [[row.row_id, row.partition, row.reading] for row in rows]]
            for key, (commit, rows) in states.items()}


def main(args):
    if args == ['probe']:
        probe()
        return 0
    if args not in (['verify'], ['demo']):
        print('usage: replica_examples.py {probe,verify,demo}', file=sys.stderr)
        return 2
    with TemporaryDirectory() as directory:
        if args == ['verify']:
            gate = CommitGate()
            with patch.object(sqlite3, 'connect', gate.connect), patch.object(sqlite3.dbapi2, 'connect', gate.connect):
                results = run_examples(Path(directory), gate)
        else:
            results = run_examples(Path(directory))
    print(json.dumps(results, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
