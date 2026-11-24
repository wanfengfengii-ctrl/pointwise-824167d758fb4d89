from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.relay.stream import relay_transaction
from warehouse.relay.windows import admission_window
from warehouse.retention.claims import find_obligation, store_review_obligation


@dataclass(frozen=True)
class Admission:
    request_id: str
    source_subscription: str
    checkpoint_id: str
    sequence: int
    review_subscription: str
    status: str

    @property
    def claim(self) -> tuple[str, str, str, int, str]:
        return (self.request_id, self.source_subscription, self.checkpoint_id,
                self.sequence, self.review_subscription)


def initialize_admissions(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS admissions (
            request_id TEXT PRIMARY KEY, source_subscription TEXT NOT NULL,
            checkpoint_id TEXT NOT NULL, sequence INTEGER NOT NULL,
            review_subscription TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL CHECK (status IN ('admitted', 'expired')),
            FOREIGN KEY (source_subscription) REFERENCES obligations (subscription)
        )
    ''')


def find_admission(connection: Connection, request_id: str) -> Admission | None:
    row = connection.execute('''
        SELECT request_id, source_subscription, checkpoint_id, sequence, review_subscription, status
        FROM admissions WHERE request_id = ?
    ''', (request_id,)).fetchone()
    return None if row is None else Admission(*row)


def admit_fork(relay, request_id: str, source_subscription: str, checkpoint_id: str,
               sequence: int, review_subscription: str, *, boundary=None) -> Admission:
    claim = (request_id, source_subscription, checkpoint_id, sequence, review_subscription)
    with relay_transaction(relay) as connection:
        existing = find_admission(connection, request_id)
        if existing is not None:
            if existing.claim != claim:
                raise ValueError(f'admission already bound for request {request_id!r}')
            return existing
        primary = find_obligation(connection, source_subscription)
        if primary is None or primary.request_id is not None:
            raise ValueError('admission requires a primary subscription')
        window = admission_window(connection, sequence)
        status = 'expired' if window is None else 'admitted'
        result = Admission(*claim, status)
        connection.execute('''
            INSERT INTO admissions (request_id, source_subscription, checkpoint_id,
                                    sequence, review_subscription, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (*result.claim, result.status))
        if window is not None:
            store_review_obligation(connection, review_subscription, request_id,
                                    window.confirmed_through)
        if boundary is not None:
            boundary('before_fork_admission_commit')
    return result
