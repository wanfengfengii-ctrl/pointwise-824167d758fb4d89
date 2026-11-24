from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.source.deltas import DeltaEntry, decode_deltas, encode_deltas


@dataclass(frozen=True)
class RelayEntry:
    sequence: int
    batch_id: str
    deltas: tuple[DeltaEntry, ...]


def initialize_entries(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS relay_entries (
            sequence INTEGER PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE, deltas TEXT NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES published_batches (batch_id)
        )
    ''')


def append_entry(connection: Connection, entry: RelayEntry) -> None:
    connection.execute('INSERT INTO relay_entries (sequence, batch_id, deltas) VALUES (?, ?, ?)',
                       (entry.sequence, entry.batch_id, encode_deltas(entry.deltas)))


def _decode(row) -> RelayEntry:
    sequence, batch_id, payload = row
    return RelayEntry(sequence, batch_id, decode_deltas(payload))


def load_entry(connection: Connection, sequence: int) -> RelayEntry | None:
    row = connection.execute('''
        SELECT sequence, batch_id, deltas FROM relay_entries WHERE sequence = ?
    ''', (sequence,)).fetchone()
    return None if row is None else _decode(row)


def list_entries_through(connection: Connection, through: int) -> tuple[RelayEntry, ...]:
    rows = connection.execute('''
        SELECT sequence, batch_id, deltas FROM relay_entries
        WHERE sequence <= ? ORDER BY sequence
    ''', (through,))
    return tuple(_decode(row) for row in rows)


def delete_entries_through(connection: Connection, through: int) -> tuple[str, ...]:
    rows = connection.execute('''
        SELECT batch_id FROM relay_entries WHERE sequence <= ? ORDER BY sequence
    ''', (through,))
    batch_ids = tuple(row[0] for row in rows)
    deleted = connection.execute('DELETE FROM relay_entries WHERE sequence <= ?', (through,))
    if deleted.rowcount != len(batch_ids):
        raise RuntimeError('work records changed within the reclaim transaction')
    return batch_ids
