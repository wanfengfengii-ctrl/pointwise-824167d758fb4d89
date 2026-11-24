from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.ledger.checkpoints import Checkpoint


@dataclass(frozen=True)
class ForkRequest:
    request_id: str
    source_subscription: str
    checkpoint_id: str
    sequence: int
    review_subscription: str
    state: str

    @property
    def binding(self) -> tuple[str, str, str, int, str]:
        return (self.request_id, self.source_subscription, self.checkpoint_id,
                self.sequence, self.review_subscription)


def initialize_requests(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS fork_requests (
            request_id TEXT PRIMARY KEY, source_subscription TEXT NOT NULL,
            checkpoint_id TEXT NOT NULL, sequence INTEGER NOT NULL,
            review_subscription TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL CHECK (state IN ('pending', 'ready', 'expired')),
            FOREIGN KEY (source_subscription, checkpoint_id)
                REFERENCES checkpoints (subscription, checkpoint_id)
        )
    ''')


def find_request(connection: Connection, request_id: str) -> ForkRequest | None:
    row = connection.execute('''
        SELECT request_id, source_subscription, checkpoint_id, sequence, review_subscription, state
        FROM fork_requests WHERE request_id = ?
    ''', (request_id,)).fetchone()
    return None if row is None else ForkRequest(*row)


def bind_request(connection: Connection, request_id: str, checkpoint: Checkpoint) -> ForkRequest:
    result = ForkRequest(request_id, checkpoint.subscription, checkpoint.checkpoint_id,
                         checkpoint.sequence, f'review:{request_id}', 'pending')
    connection.execute('''
        INSERT INTO fork_requests (request_id, source_subscription, checkpoint_id,
                                  sequence, review_subscription, state)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (*result.binding, result.state))
    return result


def _finish_request(connection: Connection, request_id: str, state: str) -> None:
    changed = connection.execute('''
        UPDATE fork_requests SET state = ? WHERE request_id = ? AND state = 'pending'
    ''', (state, request_id))
    if changed.rowcount != 1:
        raise RuntimeError(f'request {request_id!r} is no longer pending')


def mark_request_expired(connection: Connection, request_id: str) -> None:
    _finish_request(connection, request_id, 'expired')


def mark_request_ready(connection: Connection, request_id: str) -> None:
    _finish_request(connection, request_id, 'ready')
