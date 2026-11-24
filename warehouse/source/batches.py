import json
from collections.abc import Mapping
from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.source.deltas import DeltaEntry, decode_deltas, encode_deltas
from warehouse.source.facts import StationDayRevision, encode_revision, parse_revision
from warehouse.source.moves import (MoveOrder, canonical_move_body, decode_move_payload,
                                    encode_move_payload, parse_move)
from warehouse.source.origins import require_text


@dataclass(frozen=True)
class BatchInput:
    batch_id: str
    revisions: tuple[StationDayRevision, ...] = ()
    move_order: MoveOrder | None = None

    @property
    def canonical_body(self) -> str:
        if self.move_order is not None:
            return canonical_move_body(self.move_order.as_input())
        return json.dumps([encode_revision(item) for item in self.revisions],
                          sort_keys=True, separators=(',', ':'))


@dataclass(frozen=True)
class PublishedBatch:
    batch_id: str
    sequence: int
    deltas: tuple[DeltaEntry, ...]
    move_order: MoveOrder | None = None

    @property
    def move(self) -> dict | None:
        return None if self.move_order is None else self.move_order.as_input()


def canonical_batch(value) -> BatchInput:
    if not isinstance(value, Mapping) or 'batch_id' not in value:
        raise ValueError('batch requires batch_id and revisions')
    batch_id = require_text(value['batch_id'], 'batch_id')
    keys = set(value)
    if keys == {'batch_id', 'revisions'}:
        items = value['revisions']
        if not isinstance(items, (list, tuple)) or not items:
            raise ValueError('a batch requires ordered revisions')
        return BatchInput(batch_id, tuple(parse_revision(item) for item in items))
    if keys == {'batch_id', 'move'}:
        return BatchInput(batch_id, (), parse_move(value['move']))
    raise ValueError('batch requires batch_id and revisions')


def initialize_batches(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS published_batches (
            batch_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE,
            canonical_body TEXT NOT NULL, deltas TEXT NOT NULL, move_payload TEXT
        )
    ''')
    existing = {row[1] for row in connection.execute('PRAGMA table_info(published_batches)')}
    if 'move_payload' not in existing:
        connection.execute('ALTER TABLE published_batches ADD COLUMN move_payload TEXT')


def find_published_batch(connection: Connection,
                         batch_id: str) -> tuple[PublishedBatch, str] | None:
    row = connection.execute('''
        SELECT sequence, canonical_body, deltas, move_payload
        FROM published_batches WHERE batch_id = ?
    ''', (batch_id,)).fetchone()
    if row is None:
        return None
    sequence, body, deltas, move_payload = row
    move_order = None if move_payload is None else decode_move_payload(move_payload)
    return PublishedBatch(batch_id, sequence, decode_deltas(deltas), move_order), body


def save_published_batch(connection: Connection, batch: PublishedBatch, body: str) -> None:
    move_payload = None if batch.move_order is None else encode_move_payload(batch.move_order)
    connection.execute('''
        INSERT INTO published_batches (batch_id, sequence, canonical_body, deltas, move_payload)
        VALUES (?, ?, ?, ?, ?)
    ''', (batch.batch_id, batch.sequence, body, encode_deltas(batch.deltas), move_payload))
