from dataclasses import dataclass

from warehouse.fork.receipts import ForkReceipt, find_receipt, save_receipt
from warehouse.fork.requests import ForkRequest, find_request, mark_request_expired, mark_request_ready
from warehouse.ledger.checkpoints import load_checkpoint
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import seed_subscription
from warehouse.relay.admit import admit_fork


@dataclass(frozen=True)
class RecoveryResult:
    status: str
    receipt: ForkReceipt | None = None


def inspect_fork(connection, request: ForkRequest) -> RecoveryResult:
    if request.state == 'pending':
        return RecoveryResult('waiting')
    if request.state == 'expired':
        return RecoveryResult('expired')
    receipt = find_receipt(connection, request.request_id)
    if receipt is None:
        raise RuntimeError(f'ready request {request.request_id!r} has no first receipt')
    return RecoveryResult('ready', receipt)


def _check_binding(connection, request: ForkRequest) -> ForkRequest:
    current = find_request(connection, request.request_id)
    if current is None or current.binding != request.binding:
        raise ValueError(f'no matching durable fork request {request.request_id!r}')
    return current


def resume_fork(relay, ledger, request: ForkRequest, *, boundary=None) -> RecoveryResult:
    with ledger_transaction(ledger, write=False) as connection:
        current = _check_binding(connection, request)
        if current.state != 'pending':
            return inspect_fork(connection, current)
    admission = admit_fork(relay, *request.binding, boundary=boundary)
    if boundary is not None:
        boundary('after_fork_admission')
    if admission.claim != request.binding:
        raise RuntimeError('relay admission does not match the fixed fork request')
    with ledger_transaction(ledger) as connection:
        current = _check_binding(connection, request)
        if current.state != 'pending':
            return inspect_fork(connection, current)
        if admission.status == 'expired':
            mark_request_expired(connection, request.request_id)
            result = RecoveryResult('expired')
        else:
            checkpoint = load_checkpoint(connection, request.source_subscription, request.checkpoint_id)
            if checkpoint.sequence != request.sequence:
                raise RuntimeError('fork checkpoint changed sequence after binding')
            review = seed_subscription(connection, request.review_subscription, checkpoint)
            receipt = ForkReceipt(request.request_id, request.source_subscription,
                                  request.checkpoint_id, review.subscription,
                                  review.checkpoint_id, review.sequence)
            save_receipt(connection, receipt)
            mark_request_ready(connection, request.request_id)
            result = RecoveryResult('ready', receipt)
        if boundary is not None:
            boundary('before_fork_finalize_commit')
    if boundary is not None:
        boundary('after_fork_finalize')
    return result
