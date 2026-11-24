import sqlite3
from contextlib import closing, contextmanager


def open_ledger(path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute('PRAGMA foreign_keys = ON')
    return connection


@contextmanager
def ledger_transaction(path, *, write: bool = True):
    with closing(open_ledger(path)) as connection, connection:
        connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        yield connection
