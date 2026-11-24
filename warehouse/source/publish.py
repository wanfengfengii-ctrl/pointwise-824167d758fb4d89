from sqlite3 import Connection

from warehouse.relay.entries import RelayEntry, append_entry
from warehouse.relay.stream import allocate_sequence, relay_transaction
from warehouse.source.batches import (PublishedBatch, canonical_batch, find_published_batch,
                                      initialize_batches, save_published_batch)
from warehouse.source.deltas import compute_deltas
from warehouse.source.facts import initialize_facts, resolve_revisions, save_revisions


def initialize_source(connection: Connection) -> None:
    initialize_batches(connection)
    initialize_facts(connection)


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
        changes = resolve_revisions(connection, incoming.revisions)
        deltas = compute_deltas(changes)
        sequence = allocate_sequence(connection)
        result = PublishedBatch(incoming.batch_id, sequence, deltas)
        save_published_batch(connection, result, body)
        save_revisions(connection, sequence, changes)
        append_entry(connection, RelayEntry(sequence, result.batch_id, deltas))
        if boundary is not None:
            boundary('before_publish_commit')
    return result
