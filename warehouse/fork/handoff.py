import sqlite3
from dataclasses import dataclass

from warehouse.fork.receipts import ForkReceipt
from warehouse.fork.recovery import inspect_fork, resume_fork
from warehouse.fork.requests import bind_request, find_request
from warehouse.ledger.checkpoints import load_checkpoint
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import find_subscription
from warehouse.source.origins import require_text


@dataclass(frozen=True)
class ForkResult:
    status: str
    request_id: str
    receipt: ForkReceipt | None = None


def fork_status(ledger, request_id: str) -> ForkResult:
    with ledger_transaction(ledger, write=False) as connection:
        request = find_request(connection, request_id)
        if request is None:
            raise KeyError(f'unknown fork request {request_id!r}')
        result = inspect_fork(connection, request)
    return ForkResult(result.status, request_id, result.receipt)


def fork(relay, ledger, request_id: str, source_subscription: str, checkpoint_id: str,
         *, boundary=None) -> ForkResult:
    require_text(request_id, 'request_id')
    require_text(source_subscription, 'source_subscription')
    require_text(checkpoint_id, 'checkpoint_id')
    try:
        with ledger_transaction(ledger) as connection:
            request = find_request(connection, request_id)
            if request is None:
                checkpoint = load_checkpoint(connection, source_subscription, checkpoint_id)
                source = find_subscription(connection, source_subscription)
                if source.kind != 'primary':
                    raise ValueError('fork source must be the primary subscription')
                request = bind_request(connection, request_id, checkpoint)
            elif (request.source_subscription, request.checkpoint_id) != (source_subscription, checkpoint_id):
                raise ValueError(f'fork request {request_id!r} was used with different parameters')
            if request.state != 'pending':
                result = inspect_fork(connection, request)
                return ForkResult(result.status, request_id, result.receipt)
        if boundary is not None:
            boundary('after_fork_binding')
        result = resume_fork(relay, ledger, request, boundary=boundary)
        return ForkResult(result.status, request_id, result.receipt)
    except TimeoutError:
        return ForkResult('waiting', request_id)
    except sqlite3.OperationalError as error:
        code = getattr(error, 'sqlite_errorcode', None)
        if code is not None and (code & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            return ForkResult('waiting', request_id)
        raise
