from contextlib import contextmanager
import os
import sqlite3


MAX_SQLITE_INTEGER = (1 << 63) - 1
DEFAULT_RETENTION = 512

_SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS lake_meta (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        origin INTEGER NOT NULL,
        head INTEGER NOT NULL CHECK (head >= origin),
        generation INTEGER NOT NULL,
        retention INTEGER NOT NULL CHECK (retention > 0)
    )''',
    '''CREATE TABLE IF NOT EXISTS current_rows (
        row_id TEXT PRIMARY KEY,
        partition_name TEXT NOT NULL,
        reading TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS segments (
        segment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_commit INTEGER UNIQUE,
        base_count INTEGER NOT NULL CHECK (base_count >= 0),
        edit_count INTEGER NOT NULL CHECK (edit_count >= 0)
    )''',
    '''CREATE TABLE IF NOT EXISTS segment_mutations (
        segment_id INTEGER NOT NULL REFERENCES segments ON DELETE CASCADE,
        row_id TEXT NOT NULL,
        commit_no INTEGER NOT NULL,
        partition_name TEXT,
        reading TEXT,
        CHECK ((partition_name IS NULL) = (reading IS NULL)),
        PRIMARY KEY (segment_id, row_id)
    )''',
    '''CREATE TABLE IF NOT EXISTS segment_base (
        segment_id INTEGER NOT NULL REFERENCES segments ON DELETE CASCADE,
        row_id TEXT NOT NULL,
        partition_name TEXT NOT NULL,
        reading TEXT NOT NULL,
        PRIMARY KEY (segment_id, row_id)
    )''',
    '''CREATE TABLE IF NOT EXISTS segment_edits (
        segment_id INTEGER NOT NULL REFERENCES segments ON DELETE CASCADE,
        ordinal INTEGER NOT NULL,
        operation TEXT NOT NULL,
        row_id TEXT NOT NULL,
        partition_name TEXT,
        reading TEXT,
        PRIMARY KEY (segment_id, ordinal)
    )''',
    '''CREATE TABLE IF NOT EXISTS active_segments (
        segment_id INTEGER PRIMARY KEY REFERENCES segments ON DELETE RESTRICT
    )''',
    '''CREATE TABLE IF NOT EXISTS commit_history (
        commit_no INTEGER PRIMARY KEY,
        source_segment INTEGER REFERENCES segments ON DELETE SET NULL
    )''',
    '''CREATE INDEX IF NOT EXISTS history_by_segment
        ON commit_history (source_segment, commit_no)''',
    '''CREATE TABLE IF NOT EXISTS history_archive (
        commit_no INTEGER PRIMARY KEY REFERENCES commit_history ON DELETE CASCADE,
        changes TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS receipts (
        batch_id TEXT PRIMARY KEY,
        request_json TEXT NOT NULL,
        commit_no INTEGER NOT NULL UNIQUE
    )''',
    '''CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id TEXT PRIMARY KEY,
        commit_no INTEGER NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS snapshot_sources (
        snapshot_id TEXT NOT NULL REFERENCES snapshots ON DELETE CASCADE,
        segment_id INTEGER NOT NULL REFERENCES segments ON DELETE RESTRICT,
        PRIMARY KEY (snapshot_id, segment_id)
    )''',
    '''CREATE INDEX IF NOT EXISTS snapshots_by_segment
        ON snapshot_sources (segment_id, snapshot_id)''',
    '''CREATE TABLE IF NOT EXISTS replacements (
        replacement_id INTEGER PRIMARY KEY AUTOINCREMENT,
        generation INTEGER NOT NULL UNIQUE,
        new_segment INTEGER NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS replacement_inputs (
        replacement_id INTEGER NOT NULL REFERENCES replacements,
        segment_id INTEGER NOT NULL,
        eligible_at_publish INTEGER NOT NULL CHECK (eligible_at_publish IN (0, 1)),
        last_status TEXT NOT NULL DEFAULT 'pending',
        last_reasons TEXT NOT NULL DEFAULT '[]',
        PRIMARY KEY (replacement_id, segment_id)
    )''',
    '''CREATE TABLE IF NOT EXISTS subscriptions (
        subscription_id INTEGER PRIMARY KEY AUTOINCREMENT,
        partition_name TEXT NOT NULL,
        start_commit INTEGER NOT NULL,
        end_commit INTEGER NOT NULL CHECK (end_commit >= start_commit),
        page_size TEXT NOT NULL
    )''',
)


@contextmanager
def _transaction(db, begin):
    db.execute(begin)
    try:
        yield db
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise


def read_transaction(db):
    return _transaction(db, 'BEGIN')


def write_transaction(db):
    return _transaction(db, 'BEGIN IMMEDIATE')


def _check_setting(value, name):
    if value is not None and (type(value) is not int or not 1 <= value <= MAX_SQLITE_INTEGER):
        raise ValueError(f'{name} must be a positive SQLite integer')


def initialize_database(db, *, first_commit=None, retention=None):
    _check_setting(first_commit, 'first_commit')
    _check_setting(retention, 'retention')
    with write_transaction(db):
        for statement in _SCHEMA:
            db.execute(statement)
        settings = db.execute('SELECT origin, retention FROM lake_meta WHERE singleton = 1').fetchone()
        if settings is None:
            origin = (first_commit if first_commit is not None else 1) - 1
            keep = retention if retention is not None else DEFAULT_RETENTION
            db.execute('INSERT INTO lake_meta VALUES (1, ?, ?, 0, ?)', (origin, origin, keep))
        else:
            origin, keep = settings
            if first_commit is not None and first_commit - 1 != origin:
                raise ValueError('first_commit differs from the initialized lake')
            if retention is not None and retention != keep:
                raise ValueError('retention differs from the initialized lake')


def open_database(path: str | os.PathLike, *, first_commit=None, retention=None) -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys = ON')
        initialize_database(db, first_commit=first_commit, retention=retention)
    except BaseException:
        db.close()
        raise
    return db
