from dataclasses import dataclass
from sqlite3 import Connection


@dataclass(frozen=True)
class ForkReceipt:
    request_id: str
    source_subscription: str
    source_checkpoint: str
    review_subscription: str
    initial_checkpoint: str
    sequence: int


def initialize_receipts(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS fork_receipts (
            request_id TEXT PRIMARY KEY, source_subscription TEXT NOT NULL,
            source_checkpoint TEXT NOT NULL, review_subscription TEXT NOT NULL UNIQUE,
            initial_checkpoint TEXT NOT NULL, sequence INTEGER NOT NULL,
            FOREIGN KEY (request_id) REFERENCES fork_requests (request_id),
            FOREIGN KEY (review_subscription, initial_checkpoint)
                REFERENCES checkpoints (subscription, checkpoint_id)
        )
    ''')


def find_receipt(connection: Connection, request_id: str) -> ForkReceipt | None:
    row = connection.execute('''
        SELECT request_id, source_subscription, source_checkpoint, review_subscription,
               initial_checkpoint, sequence FROM fork_receipts WHERE request_id = ?
    ''', (request_id,)).fetchone()
    return None if row is None else ForkReceipt(*row)


def save_receipt(connection: Connection, receipt: ForkReceipt) -> None:
    connection.execute('''
        INSERT INTO fork_receipts (request_id, source_subscription, source_checkpoint,
                                  review_subscription, initial_checkpoint, sequence)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (receipt.request_id, receipt.source_subscription, receipt.source_checkpoint,
          receipt.review_subscription, receipt.initial_checkpoint, receipt.sequence))
