from dataclasses import dataclass

from lake.history.records import CommitEvidence
from lake.history.store import HistoryUnavailable, read_commit_evidence, read_history_bounds


class CoverageError(ValueError):
    pass


class ExpiredWindow(CoverageError):
    pass


@dataclass(frozen=True, slots=True)
class CoverageWindow:
    floor: int
    head: int


def _check_bounds(db, start: int, end: int) -> CoverageWindow:
    if type(start) is not int or type(end) is not int or start < 0 or end < start:
        raise ValueError('expected nonnegative commit endpoints with start <= end')
    window = CoverageWindow(*read_history_bounds(db))
    if start < window.floor:
        raise ExpiredWindow(f'interval starts at {start}, below retained start {window.floor}')
    if end > window.head:
        raise CoverageError(f'interval ends at {end}, beyond committed head {window.head}')
    return window


def _frames(db, start, end):
    for commit_no in range(start + 1, end + 1):
        try:
            frame = read_commit_evidence(db, commit_no)
        except HistoryUnavailable as exc:
            raise CoverageError(f'interval lacks evidence for commit {commit_no}') from exc
        yield frame


def check_coverage(db, start: int, end: int) -> CoverageWindow:
    window = _check_bounds(db, start, end)
    for _ in _frames(db, start, end):
        pass
    return window


def read_covered_interval(db, start: int, end: int) -> tuple[CommitEvidence, ...]:
    _check_bounds(db, start, end)
    return tuple(_frames(db, start, end))
