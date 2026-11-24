from dataclasses import dataclass

from warehouse.ledger.history import find_posted_batch
from warehouse.ledger.store import ledger_transaction
from warehouse.relay.entries import load_entry
from warehouse.relay.stream import relay_transaction
from warehouse.retention.claims import advance_obligation, find_obligation


@dataclass(frozen=True)
class AckResult:
    subscription: str
    confirmed_through: int
    advanced: bool


def acknowledge_posted(relay, ledger, subscription: str, batch_id: str, sequence: int) -> AckResult:
    # A committed history row is immutable and can be checked before taking the relay write lock.
    with ledger_transaction(ledger, write=False) as connection:
        posted = find_posted_batch(connection, subscription, batch_id)
    if posted is None or posted.sequence != sequence:
        raise ValueError('acknowledgment requires the matching committed posting')
    with relay_transaction(relay) as connection:
        obligation = find_obligation(connection, subscription)
        if obligation is None:
            raise KeyError(f'unknown relay subscription {subscription!r}')
        if sequence <= obligation.confirmed_through:
            return AckResult(subscription, obligation.confirmed_through, False)
        if sequence != obligation.confirmed_through + 1:
            raise ValueError('acknowledgment would skip a batch')
        entry = load_entry(connection, sequence)
        if entry is None or (entry.batch_id, entry.deltas) != (batch_id, posted.deltas):
            raise ValueError('committed posting does not match the next relay batch')
        advance_obligation(connection, obligation, sequence)
    return AckResult(subscription, sequence, True)
