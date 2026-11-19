from dataclasses import dataclass
import json

from lake.compact.eligibility import assess_retirement
from lake.compact.publish import load_replacement
from lake.table.segments import delete_segment
from lake.table.storage import write_transaction


@dataclass(frozen=True, slots=True)
class RetirementResult:
    segment_id: int
    status: str
    reasons: tuple[str, ...] = ()


def retire(db, replacement_id: int) -> tuple[RetirementResult, ...]:
    with write_transaction(db):
        replacement = load_replacement(db, replacement_id)
        prior = dict(db.execute(
            'SELECT segment_id, last_status FROM replacement_inputs WHERE replacement_id = ?',
            (replacement_id,),
        ))
        eligibility = assess_retirement(db, replacement.original_ids)
        results = []
        for item in eligibility:
            if prior[item.segment_id] == 'retired':
                results.append(RetirementResult(item.segment_id, 'already_retired'))
                continue
            if item.eligible:
                delete_segment(db, item.segment_id)
                result = RetirementResult(item.segment_id, 'retired')
            else:
                result = RetirementResult(item.segment_id, 'blocked', item.reasons)
            db.execute(
                '''UPDATE replacement_inputs SET last_status = ?, last_reasons = ?
                WHERE replacement_id = ? AND segment_id = ?''',
                (result.status, json.dumps(result.reasons), replacement_id, item.segment_id),
            )
            results.append(result)
        return tuple(results)
