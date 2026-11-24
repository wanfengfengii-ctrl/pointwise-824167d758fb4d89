from dataclasses import dataclass

from warehouse.source.deltas import DeltaEntry, decode_deltas, encode_deltas
from warehouse.source.moves import MoveOrder, decode_move_payload, encode_move_payload


@dataclass(frozen=True)
class RelayEntry:
    sequence: int
    batch_id: str
    deltas: tuple[DeltaEntry, ...]
    move_order: MoveOrder | None = None


def initialize_entries(connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS relay_entries (
            sequence INTEGER PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE, deltas TEXT NOT NULL,
            move_payload TEXT,
            FOREIGN KEY (batch_id) REFERENCES published_batches (batch_id)
        )
    ''')
    existing = {row[1] for row in connection.execute('PRAGMA table_info(relay_entries)')}
    if 'move_payload' not in existing:
        connection.execute('ALTER TABLE relay_entries ADD COLUMN move_payload TEXT')


def append_entry(connection, entry: RelayEntry) -> None:
    move_payload = None if entry.move_order is None else encode_move_payload(entry.move_order)
    connection.execute('''
        INSERT INTO relay_entries (sequence, batch_id, deltas, move_payload)
        VALUES (?, ?, ?, ?)
    ''', (entry.sequence, entry.batch_id, encode_deltas(entry.deltas), move_payload))


def _decode(row) -> RelayEntry:
    sequence, batch_id, payload, move_payload = row
    move_order = None if move_payload is None else decode_move_payload(move_payload)
    return RelayEntry(sequence, batch_id, decode_deltas(payload), move_order)


def load_entry(connection, sequence: int) -> RelayEntry | None:
    row = connection.execute('''
        SELECT sequence, batch_id, deltas, move_payload FROM relay_entries WHERE sequence = ?
    ''', (sequence,)).fetchone()
    return None if row is None else _decode(row)


def list_entries_through(connection, through: int) -> tuple[RelayEntry, ...]:
    rows = connection.execute('''
        SELECT sequence, batch_id, deltas, move_payload FROM relay_entries
        WHERE sequence <= ? ORDER BY sequence
    ''', (through,))
    return tuple(_decode(row) for row in rows)


def delete_entries_through(connection, through: int) -> tuple[str, ...]:
    rows = connection.execute('''
        SELECT batch_id FROM relay_entries WHERE sequence <= ? ORDER BY sequence
    ''', (through,))
    batch_ids = tuple(row[0] for row in rows)
    deleted = connection.execute('DELETE FROM relay_entries WHERE sequence <= ?', (through,))
    if deleted.rowcount != len(batch_ids):
        raise RuntimeError('work records changed within the reclaim transaction')
    return batch_ids
