from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.ledger.balances import save_balances
from warehouse.ledger.checkpoints import Checkpoint, save_checkpoint, seed_checkpoint_prefix
from warehouse.ledger.history import seed_history


@dataclass(frozen=True)
class LedgerSubscription:
    subscription: str
    kind: str
    sequence: int
    checkpoint_id: str


def initialize_subscriptions(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('primary', 'review')),
            sequence INTEGER NOT NULL CHECK (sequence >= 0), checkpoint_id TEXT NOT NULL
        )
    ''')
    connection.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS one_primary_ledger
        ON subscriptions (kind) WHERE kind = 'primary'
    ''')


def find_subscription(connection: Connection, subscription: str) -> LedgerSubscription | None:
    row = connection.execute('''
        SELECT subscription, kind, sequence, checkpoint_id FROM subscriptions WHERE subscription = ?
    ''', (subscription,)).fetchone()
    return None if row is None else LedgerSubscription(*row)


def ensure_primary_ledger(connection: Connection, subscription: str) -> LedgerSubscription:
    row = connection.execute('''
        SELECT subscription, kind, sequence, checkpoint_id FROM subscriptions WHERE kind = 'primary'
    ''').fetchone()
    if row is not None:
        current = LedgerSubscription(*row)
        if current.subscription != subscription:
            raise ValueError(f'ledger already belongs to primary {current.subscription!r}')
        return current
    connection.execute('''
        INSERT INTO subscriptions (subscription, kind, sequence, checkpoint_id)
        VALUES (?, 'primary', 0, 'C0')
    ''', (subscription,))
    save_checkpoint(connection, Checkpoint(subscription, 'C0', 0, None, ()))
    return LedgerSubscription(subscription, 'primary', 0, 'C0')


def seed_subscription(connection: Connection, subscription: str,
                      source: Checkpoint) -> LedgerSubscription:
    connection.execute('''
        INSERT INTO subscriptions (subscription, kind, sequence, checkpoint_id)
        VALUES (?, 'review', ?, ?)
    ''', (subscription, source.sequence, source.checkpoint_id))
    seed_checkpoint_prefix(connection, source, subscription)
    seed_history(connection, source.subscription, subscription, source.sequence)
    save_balances(connection, subscription, source.balances)
    return LedgerSubscription(subscription, 'review', source.sequence, source.checkpoint_id)


def advance_subscription(connection: Connection, subscription: LedgerSubscription,
                         checkpoint: Checkpoint) -> None:
    if (checkpoint.subscription != subscription.subscription or
            checkpoint.sequence != subscription.sequence + 1):
        raise ValueError('checkpoint does not follow the subscription head')
    changed = connection.execute('''
        UPDATE subscriptions SET sequence = ?, checkpoint_id = ?
        WHERE subscription = ? AND sequence = ?
    ''', (checkpoint.sequence, checkpoint.checkpoint_id, subscription.subscription,
          subscription.sequence))
    if changed.rowcount != 1:
        raise RuntimeError('ledger head changed within the posting transaction')
