from contextlib import closing
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from lake.compact.publish import compact
from lake.compact.retire import retire
from lake.feed.delivery import read_page
from lake.feed.subscription import open_feed
from lake.ingest.admission import apply
from lake.ingest.batch import BatchRequest, Edit
from lake.table.catalog import load_catalog
from lake.table.segments import SegmentUnavailable, read_segment
from lake.table.snapshot import register_snapshot, snapshot
from lake.table.storage import open_database


def read_sample():
    path = Path(__file__).resolve().parent / 'samples' / 'linchuan.json'
    with path.open(encoding='utf-8') as stream:
        data = json.load(stream)
    requests = tuple(BatchRequest(item['id'], tuple(Edit(*edit) for edit in item['edits']))
                     for item in data['batches'])
    return data, requests


def change_record(change):
    before = None if change.before is None else [change.before.partition, change.before.reading]
    after = None if change.after is None else [change.after.partition, change.after.reading]
    return [change.commit_no, change.row_id, change.action, before, after]


def apply_to_replica(replica, changes):
    for change in changes:
        if replica.get(change.row_id) != change.before:
            raise ValueError(f'replica preimage mismatch for {change.row_id!r}')
        if change.after is None:
            del replica[change.row_id]
        else:
            replica[change.row_id] = change.after


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    data, requests = read_sample()
    opening, transfer, return_home, late = requests
    expected = [
        [41, 'r', 'remove', ['东区', 5], None],
        [42, 'r', 'add', None, ['东区', 5]],
    ]
    downstream_expected = {'after_first': [], 'after_resume': [['r', '东区', 5]], 'current': [['r', '东区', 9]]}
    with TemporaryDirectory() as directory:
        path = Path(directory) / 'linchuan.sqlite'
        with closing(open_database(path, first_commit=data['first_commit'], retention=data['retention'])) as db:
            receipt = apply(db, opening)
            require(receipt.commit_no == 40 and apply(db, opening) == receipt, 'unexpected opening receipt')
            bottom = register_snapshot(db, 'shift-40')
            require(apply(db, transfer).commit_no == 41, 'unexpected transfer receipt')
            require(apply(db, return_home).commit_no == 42, 'unexpected return receipt')
            register_snapshot(db, 'shift-42')
            start, end = data['interval']
            sub = open_feed(db, data['partition'], start, end, data['page_size'])
            first = read_page(db, sub.subscription_id, sub.first_cursor)
            require(len(first.changes) == 1 and not first.done, 'expected a resumable first page')
            replica = {row.row_id: row for row in bottom.rows if row.partition == sub.partition}
            apply_to_replica(replica, first.changes)
            first_replica = [[row.row_id, row.partition, row.reading] for row in sorted(replica.values(), key=lambda r: r.row_id)]
            originals = load_catalog(db).segment_ids[1:]
            published = compact(db, originals)
            retired = retire(db, published.replacement_id)
            require(tuple(item.segment_id for item in retired if item.status == 'retired') == originals,
                    'the selected originals were not retired')
        with closing(open_database(path)) as db:
            for segment_id in originals:
                try:
                    read_segment(db, segment_id)
                except SegmentUnavailable:
                    continue
                raise RuntimeError(f'old segment {segment_id} remains readable')
            require(apply(db, late).commit_no == 43, 'unexpected late receipt')
            second = read_page(db, sub.subscription_id, first.next_cursor)
            require(second.done and read_page(db, sub.subscription_id, first.next_cursor) == second,
                    'resumed page is not terminal and repeatable')
            apply_to_replica(replica, second.changes)
            fixed = snapshot(db, 'shift-42')
            require(fixed.commit_no == 42 and fixed.rows == tuple(replica.values()), 'replica disagrees with fixed snapshot')
            current = snapshot(db)
            require(current.commit_no == 43, 'current snapshot did not advance')
            actual = [change_record(change) for change in first.changes + second.changes]
            downstream_actual = {
                'after_first': first_replica,
                'after_resume': [[row.row_id, row.partition, row.reading] for row in fixed.rows],
                'current': [[row.row_id, row.partition, row.reading] for row in current.rows],
            }
            require(actual == expected and downstream_actual == downstream_expected, 'shift reconciliation differs from the expected ledger')
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'v5.history_handoff',
        'input': data,
        'expected': expected,
        'actual': actual,
        'downstream_expected': downstream_expected,
        'downstream_actual': downstream_actual,
        'code_paths': ['lake/table/commit.py', 'lake/history/legacy.py', 'lake/compact/rewrite.py',
                       'lake/compact/publish.py', 'lake/compact/retire.py', 'lake/history/store.py',
                       'lake/feed/changes.py', 'lake/feed/delivery.py', 'lake/table/snapshot.py'],
        'oracle': '手算东区 r 在 41 撤下、42 按原值补回。43 只改变当前底账。',
    }, ensure_ascii=False, separators=(',', ':')))


if __name__ == '__main__':
    main()
