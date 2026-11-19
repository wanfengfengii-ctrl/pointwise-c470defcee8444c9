from lake.ingest.batch import BatchRequest, request_identity
from lake.ingest.receipts import BatchReceipt, load_receipt, save_receipt
from lake.ingest.validate import validate_batch
from lake.table.commit import commit_batch
from lake.table.storage import write_transaction


class BatchConflict(ValueError):
    pass


def apply(db, request: BatchRequest) -> BatchReceipt:
    request = validate_batch(request)
    identity = request_identity(request)
    with write_transaction(db):
        existing = load_receipt(db, request.batch_id)
        if existing is not None:
            old_identity, receipt = existing
            if old_identity != identity:
                raise BatchConflict(f'batch {request.batch_id!r} has different ordered edits')
            return receipt
        commit_no = commit_batch(db, request)
        receipt = BatchReceipt(request.batch_id, commit_no)
        save_receipt(db, identity, receipt)
        return receipt
