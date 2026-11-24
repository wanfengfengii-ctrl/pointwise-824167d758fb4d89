import json
from collections.abc import Mapping
from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.source.deltas import DeltaEntry, decode_deltas, encode_deltas
from warehouse.source.facts import StationDayRevision, encode_revision, parse_revision
from warehouse.source.origins import require_text


@dataclass(frozen=True)
class BatchInput:
    batch_id: str
    revisions: tuple[StationDayRevision, ...]

    @property
    def canonical_body(self) -> str:
        return json.dumps([encode_revision(item) for item in self.revisions],
                          sort_keys=True, separators=(',', ':'))


@dataclass(frozen=True)
class PublishedBatch:
    batch_id: str
    sequence: int
    deltas: tuple[DeltaEntry, ...]


def canonical_batch(value) -> BatchInput:
    if not isinstance(value, Mapping) or set(value) != {'batch_id', 'revisions'}:
        raise ValueError('batch requires batch_id and revisions')
    batch_id = require_text(value['batch_id'], 'batch_id')
    items = value['revisions']
    if not isinstance(items, (list, tuple)) or not items:
        raise ValueError('a batch requires ordered revisions')
    return BatchInput(batch_id, tuple(parse_revision(item) for item in items))


def initialize_batches(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS published_batches (
            batch_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE,
            canonical_body TEXT NOT NULL, deltas TEXT NOT NULL
        )
    ''')


def find_published_batch(connection: Connection,
                         batch_id: str) -> tuple[PublishedBatch, str] | None:
    row = connection.execute('''
        SELECT sequence, canonical_body, deltas FROM published_batches WHERE batch_id = ?
    ''', (batch_id,)).fetchone()
    if row is None:
        return None
    sequence, body, deltas = row
    return PublishedBatch(batch_id, sequence, decode_deltas(deltas)), body


def save_published_batch(connection: Connection, batch: PublishedBatch, body: str) -> None:
    connection.execute('''
        INSERT INTO published_batches (batch_id, sequence, canonical_body, deltas)
        VALUES (?, ?, ?, ?)
    ''', (batch.batch_id, batch.sequence, body, encode_deltas(batch.deltas)))
