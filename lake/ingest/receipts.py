from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BatchReceipt:
    batch_id: str
    commit_no: int


def load_receipt(db, batch_id: str) -> tuple[str, BatchReceipt] | None:
    stored = db.execute(
        'SELECT request_json, commit_no FROM receipts WHERE batch_id = ?', (batch_id,)
    ).fetchone()
    if stored is None:
        return None
    identity, commit_no = stored
    return identity, BatchReceipt(batch_id, commit_no)


def save_receipt(db, identity: str, receipt: BatchReceipt) -> None:
    db.execute(
        'INSERT INTO receipts (batch_id, request_json, commit_no) VALUES (?, ?, ?)',
        (receipt.batch_id, identity, receipt.commit_no),
    )
