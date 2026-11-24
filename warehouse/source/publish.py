from sqlite3 import Connection

from warehouse.relay.entries import RelayEntry, append_entry
from warehouse.relay.stream import allocate_sequence, relay_transaction
from warehouse.source.batches import (PublishedBatch, canonical_batch, find_published_batch,
                                      initialize_batches, save_published_batch)
from warehouse.source.deltas import compute_deltas
from warehouse.source.facts import (initialize_facts, load_current_revision, resolve_revisions,
                                     save_revisions)
from warehouse.source.moves import (MoveOrder, initialize_move_events, revision_used_anywhere,
                                    save_move_event)


def initialize_source(connection: Connection) -> None:
    initialize_batches(connection)
    initialize_facts(connection)
    initialize_move_events(connection)


def _resolve_move(connection, move: MoveOrder) -> MoveOrder:
    source = move.source
    target = move.target
    current = load_current_revision(connection, *source.key)
    if current is None or current.revision != source.revision:
        raise ValueError(
            f'move source {source.key!r} revision {source.revision!r} is not the current revision')
    target_current = load_current_revision(connection, *target.key)
    if revision_used_anywhere(connection, *target.key, target.revision):
        raise ValueError(
            f'move target revision {target.revision!r} was already used for {target.key!r}')
    if target_current is not None:
        raise ValueError(f'move target {target.key!r} is occupied')
    return MoveOrder(source, target, move.origin, current.state, current.liters)


def publish(relay, batch, *, boundary=None) -> PublishedBatch:
    incoming = canonical_batch(batch)
    body = incoming.canonical_body
    with relay_transaction(relay) as connection:
        existing = find_published_batch(connection, incoming.batch_id)
        if existing is not None:
            original, original_body = existing
            if body != original_body:
                raise ValueError(f'batch id {incoming.batch_id!r} was used with different revisions')
            return original
        if incoming.move_order is not None:
            move = _resolve_move(connection, incoming.move_order)
            deltas = ()
            sequence = allocate_sequence(connection)
            result = PublishedBatch(incoming.batch_id, sequence, deltas, move)
            save_published_batch(connection, result, body)
            save_move_event(connection, sequence, 1, move)
        else:
            changes = resolve_revisions(connection, incoming.revisions)
            deltas = compute_deltas(changes)
            sequence = allocate_sequence(connection)
            result = PublishedBatch(incoming.batch_id, sequence, deltas)
            save_published_batch(connection, result, body)
            save_revisions(connection, sequence, changes)
        append_entry(connection, RelayEntry(sequence, result.batch_id, deltas, result.move_order))
        if boundary is not None:
            boundary('before_publish_commit')
    return result
