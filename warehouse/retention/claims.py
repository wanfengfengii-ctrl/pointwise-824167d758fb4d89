from dataclasses import dataclass
from sqlite3 import Connection


@dataclass(frozen=True)
class Obligation:
    subscription: str
    request_id: str | None
    start_sequence: int
    confirmed_through: int


def initialize_claims(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS obligations (
            subscription TEXT PRIMARY KEY, request_id TEXT UNIQUE,
            start_sequence INTEGER NOT NULL,
            confirmed_through INTEGER NOT NULL,
            CHECK (0 <= start_sequence AND start_sequence <= confirmed_through),
            CHECK (request_id IS NOT NULL OR start_sequence = 0)
        )
    ''')
    connection.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS one_primary_obligation
        ON obligations ((1)) WHERE request_id IS NULL
    ''')


def ensure_primary_obligation(connection: Connection, subscription: str) -> Obligation:
    row = connection.execute('''
        SELECT subscription, request_id, start_sequence, confirmed_through
        FROM obligations WHERE request_id IS NULL
    ''').fetchone()
    if row is not None:
        current = Obligation(*row)
        if current.subscription != subscription:
            raise ValueError(f'relay already belongs to primary {current.subscription!r}')
        return current
    connection.execute('''
        INSERT INTO obligations (subscription, request_id, start_sequence, confirmed_through)
        VALUES (?, NULL, 0, 0)
    ''', (subscription,))
    return Obligation(subscription, None, 0, 0)


def store_review_obligation(connection: Connection, subscription: str,
                            request_id: str, start_sequence: int) -> Obligation:
    connection.execute('''
        INSERT INTO obligations (subscription, request_id, start_sequence, confirmed_through)
        VALUES (?, ?, ?, ?)
    ''', (subscription, request_id, start_sequence, start_sequence))
    return Obligation(subscription, request_id, start_sequence, start_sequence)


def find_obligation(connection: Connection, subscription: str) -> Obligation | None:
    row = connection.execute('''
        SELECT subscription, request_id, start_sequence, confirmed_through
        FROM obligations WHERE subscription = ?
    ''', (subscription,)).fetchone()
    return None if row is None else Obligation(*row)


def list_obligations(connection: Connection) -> tuple[Obligation, ...]:
    rows = connection.execute('''
        SELECT subscription, request_id, start_sequence, confirmed_through
        FROM obligations ORDER BY subscription
    ''')
    return tuple(Obligation(*row) for row in rows)


def advance_obligation(connection: Connection, obligation: Obligation, sequence: int) -> None:
    if sequence != obligation.confirmed_through + 1:
        raise ValueError('confirmation must advance by one batch')
    changed = connection.execute('''
        UPDATE obligations SET confirmed_through = ?
        WHERE subscription = ? AND confirmed_through = ?
    ''', (sequence, obligation.subscription, obligation.confirmed_through))
    if changed.rowcount != 1:
        raise RuntimeError('confirmation frontier changed within the transaction')
