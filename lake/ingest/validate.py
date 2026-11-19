from lake.ingest.batch import BatchRequest, Edit


class InvalidBatch(ValueError):
    pass


def validate_text(value, name):
    if not isinstance(value, str) or not value.strip() or '\x00' in value:
        raise ValueError(f'{name} must be nonblank text without NUL')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise ValueError(f'{name} must be valid Unicode') from exc
    return value


def validate_edit(edit: Edit) -> Edit:
    if not isinstance(edit, Edit):
        raise InvalidBatch('each batch entry must be an Edit')
    try:
        validate_text(edit.row_id, 'row_id')
        if edit.operation in ('insert', 'update'):
            validate_text(edit.partition, 'partition')
            if type(edit.reading) is not int:
                raise ValueError('reading must be an exact integer')
        elif edit.operation == 'delete':
            if edit.partition is not None or edit.reading is not None:
                raise ValueError('delete takes only a row_id')
        else:
            raise ValueError('unknown edit operation')
    except ValueError as exc:
        raise InvalidBatch(str(exc)) from exc
    return edit


def validate_batch(request: BatchRequest) -> BatchRequest:
    if not isinstance(request, BatchRequest):
        raise InvalidBatch('expected a BatchRequest')
    try:
        validate_text(request.batch_id, 'batch_id')
    except ValueError as exc:
        raise InvalidBatch(str(exc)) from exc
    if not isinstance(request.edits, (tuple, list)):
        raise InvalidBatch('edits must be an ordered tuple or list')
    edits = tuple(validate_edit(edit) for edit in request.edits)
    return BatchRequest(request.batch_id, edits)
