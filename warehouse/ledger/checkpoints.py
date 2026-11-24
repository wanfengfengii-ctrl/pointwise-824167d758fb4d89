from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.ledger.balances import StationDayBalance, decode_balances, encode_balances
from warehouse.ledger.history import PostedBatch, read_history_prefix
from warehouse.ledger.store import ledger_transaction


@dataclass(frozen=True)
class Checkpoint:
    subscription: str
    checkpoint_id: str
    sequence: int
    last_batch_id: str | None
    balances: tuple[StationDayBalance, ...]

    @property
    def total_liters(self) -> int:
        return sum(item.contribution for item in self.balances)


@dataclass(frozen=True)
class CheckpointView:
    checkpoint: Checkpoint
    history: tuple[PostedBatch, ...]

    @property
    def total_liters(self) -> int:
        return self.checkpoint.total_liters


def initialize_checkpoints(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS checkpoints (
            subscription TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
            sequence INTEGER NOT NULL, last_batch_id TEXT, balances TEXT NOT NULL,
            PRIMARY KEY (subscription, checkpoint_id), UNIQUE (subscription, sequence),
            FOREIGN KEY (subscription) REFERENCES subscriptions (subscription)
        )
    ''')


def save_checkpoint(connection: Connection, checkpoint: Checkpoint) -> None:
    connection.execute('''
        INSERT INTO checkpoints (subscription, checkpoint_id, sequence, last_batch_id, balances)
        VALUES (?, ?, ?, ?, ?)
    ''', (checkpoint.subscription, checkpoint.checkpoint_id, checkpoint.sequence,
          checkpoint.last_batch_id, encode_balances(checkpoint.balances)))


def load_checkpoint(connection: Connection, subscription: str, checkpoint_id: str) -> Checkpoint:
    row = connection.execute('''
        SELECT sequence, last_batch_id, balances FROM checkpoints
        WHERE subscription = ? AND checkpoint_id = ?
    ''', (subscription, checkpoint_id)).fetchone()
    if row is None:
        exists = connection.execute('SELECT 1 FROM subscriptions WHERE subscription = ?',
                                    (subscription,)).fetchone()
        if exists is None:
            raise KeyError(f'unknown ledger subscription {subscription!r}')
        raise KeyError(f'unknown checkpoint {checkpoint_id!r} for subscription {subscription!r}')
    sequence, last_batch_id, body = row
    return Checkpoint(subscription, checkpoint_id, sequence, last_batch_id, decode_balances(body))


def seed_checkpoint_prefix(connection: Connection, source: Checkpoint, target: str) -> None:
    connection.execute('''
        INSERT INTO checkpoints (subscription, checkpoint_id, sequence, last_batch_id, balances)
        SELECT ?, checkpoint_id, sequence, last_batch_id, balances FROM checkpoints
        WHERE subscription = ? AND sequence <= ?
    ''', (target, source.subscription, source.sequence))


def read(ledger, subscription: str, checkpoint_id: str) -> CheckpointView:
    with ledger_transaction(ledger, write=False) as connection:
        checkpoint = load_checkpoint(connection, subscription, checkpoint_id)
        history = read_history_prefix(connection, subscription, checkpoint.sequence)
    return CheckpointView(checkpoint, history)
