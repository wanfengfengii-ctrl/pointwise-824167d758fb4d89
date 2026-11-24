import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass


@dataclass(frozen=True)
class StreamBounds:
    published_through: int
    reclaimed_through: int


def open_relay(path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute('PRAGMA foreign_keys = ON')
    return connection


@contextmanager
def relay_transaction(path, *, write: bool = True):
    with closing(open_relay(path)) as connection, connection:
        connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        yield connection


def initialize_stream(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS stream_bounds (
            stream INTEGER PRIMARY KEY CHECK (stream = 1),
            published_through INTEGER NOT NULL,
            reclaimed_through INTEGER NOT NULL,
            CHECK (0 <= reclaimed_through AND reclaimed_through <= published_through)
        )
    ''')
    connection.execute('INSERT OR IGNORE INTO stream_bounds VALUES (1, 0, 0)')


def read_stream_bounds(connection: sqlite3.Connection) -> StreamBounds:
    row = connection.execute('''
        SELECT published_through, reclaimed_through FROM stream_bounds WHERE stream = 1
    ''').fetchone()
    if row is None:
        raise RuntimeError('relay stream has not been initialized')
    return StreamBounds(*row)


def allocate_sequence(connection: sqlite3.Connection) -> int:
    sequence = read_stream_bounds(connection).published_through + 1
    connection.execute('UPDATE stream_bounds SET published_through = ? WHERE stream = 1',
                       (sequence,))
    return sequence


def set_reclaimed_through(connection: sqlite3.Connection, through: int) -> None:
    changed = connection.execute('''
        UPDATE stream_bounds SET reclaimed_through = ?
        WHERE stream = 1 AND reclaimed_through <= ? AND ? <= published_through
    ''', (through, through, through))
    if changed.rowcount != 1:
        raise RuntimeError('invalid reclaimed boundary')
