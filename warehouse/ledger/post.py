from dataclasses import dataclass

from warehouse.ledger.balances import apply_deltas, load_balances, save_balances
from warehouse.ledger.checkpoints import Checkpoint, load_checkpoint, save_checkpoint
from warehouse.ledger.history import PostedBatch, append_posted_batch, find_posted_batch
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import advance_subscription, find_subscription
from warehouse.relay.delivery import next_delivery
from warehouse.relay.entries import RelayEntry
from warehouse.retention.acks import AckResult, acknowledge_posted


@dataclass(frozen=True)
class PostResult:
    status: str
    checkpoint: Checkpoint | None = None
    batch: PostedBatch | None = None
    acknowledgment: AckResult | None = None


def _require_subscription(connection, subscription):
    current = find_subscription(connection, subscription)
    if current is None:
        raise KeyError(f'unknown ledger subscription {subscription!r}')
    return current


def post(relay, ledger, subscription: str, *, delivery: RelayEntry | None = None,
         boundary=None) -> PostResult:
    with ledger_transaction(ledger, write=False) as connection:
        _require_subscription(connection, subscription)
    offered = next_delivery(relay, subscription).entry
    entry = offered if delivery is None else delivery
    if entry is None:
        return PostResult('waiting')
    if not isinstance(entry, RelayEntry):
        raise ValueError('delivery must be a relay entry')

    with ledger_transaction(ledger) as connection:
        current = _require_subscription(connection, subscription)
        batch = find_posted_batch(connection, subscription, entry.batch_id)
        if batch is not None:
            if (batch.sequence, batch.deltas) != (entry.sequence, entry.deltas):
                raise ValueError('replayed delivery does not match the original posting')
            checkpoint = load_checkpoint(connection, subscription, batch.checkpoint_id)
            status = 'replayed'
        else:
            if entry != offered or entry.sequence != current.sequence + 1:
                raise ValueError('delivery is not the next official batch for this subscription')
            balances = apply_deltas(load_balances(connection, subscription), entry.deltas)
            checkpoint = Checkpoint(subscription, f'C{entry.sequence}', entry.sequence,
                                    entry.batch_id, balances)
            batch = PostedBatch(subscription, entry.sequence, entry.batch_id,
                                checkpoint.checkpoint_id, entry.deltas)
            save_balances(connection, subscription, balances)
            save_checkpoint(connection, checkpoint)
            append_posted_batch(connection, batch)
            advance_subscription(connection, current, checkpoint)
            status = 'posted'
            if boundary is not None:
                boundary('before_post_commit')
    if status == 'posted' and boundary is not None:
        boundary('after_post_commit')
    acknowledgment = acknowledge_posted(relay, ledger, subscription, batch.batch_id, batch.sequence)
    return PostResult(status, checkpoint, batch, acknowledgment)
