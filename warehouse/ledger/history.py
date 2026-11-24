from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.source.deltas import DeltaEntry, decode_deltas, encode_deltas


@dataclass(frozen=True)
class PostedBatch:
    subscription: str
    sequence: int
    batch_id: str
    checkpoint_id: str
    deltas: tuple[DeltaEntry, ...]


def initialize_history(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS posted_batches (
            subscription TEXT NOT NULL, sequence INTEGER NOT NULL,
            batch_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL, deltas TEXT NOT NULL,
            PRIMARY KEY (subscription, batch_id), UNIQUE (subscription, sequence),
            FOREIGN KEY (subscription, checkpoint_id)
                REFERENCES checkpoints (subscription, checkpoint_id)
        )
    ''')


def _decode(subscription: str, row) -> PostedBatch:
    sequence, batch_id, checkpoint_id, payload = row
    return PostedBatch(subscription, sequence, batch_id, checkpoint_id, decode_deltas(payload))


def find_posted_batch(connection: Connection, subscription: str,
                      batch_id: str) -> PostedBatch | None:
    row = connection.execute('''
        SELECT sequence, batch_id, checkpoint_id, deltas FROM posted_batches
        WHERE subscription = ? AND batch_id = ?
    ''', (subscription, batch_id)).fetchone()
    return None if row is None else _decode(subscription, row)


def append_posted_batch(connection: Connection, batch: PostedBatch) -> None:
    connection.execute('''
        INSERT INTO posted_batches (subscription, sequence, batch_id, checkpoint_id, deltas)
        VALUES (?, ?, ?, ?, ?)
    ''', (batch.subscription, batch.sequence, batch.batch_id, batch.checkpoint_id,
          encode_deltas(batch.deltas)))


def read_history_prefix(connection: Connection, subscription: str,
                        sequence: int) -> tuple[PostedBatch, ...]:
    rows = connection.execute('''
        SELECT sequence, batch_id, checkpoint_id, deltas FROM posted_batches
        WHERE subscription = ? AND sequence <= ? ORDER BY sequence
    ''', (subscription, sequence))
    return tuple(_decode(subscription, row) for row in rows)


def seed_history(connection: Connection, source: str, target: str, sequence: int) -> None:
    connection.execute('''
        INSERT INTO posted_batches (subscription, sequence, batch_id, checkpoint_id, deltas)
        SELECT ?, sequence, batch_id, checkpoint_id, deltas FROM posted_batches
        WHERE subscription = ? AND sequence <= ?
    ''', (target, source, sequence))
