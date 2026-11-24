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


def _ref_dict(ref) -> dict:
    return {'station': ref.station, 'day': ref.day, 'revision': ref.revision}


def _ref_matches(ref, station, day, revision) -> bool:
    return ref.station == station and ref.day == day and ref.revision == revision


def trace(ledger, subscription: str, checkpoint_id: str,
          station: str, day: str, revision: str) -> dict:
    """Reconstruct the full revision chain visible at a checkpoint.

    Every node is one revision occurrence (initial, correction or move).
    Corrections chain within a station-day; moves chain the source revision to
    the new target revision. The queried revision may have been superseded or
    moved away, as long as it was visible in the prefix history.
    """
    with ledger_transaction(ledger, write=False) as connection:
        checkpoint = load_checkpoint(connection, subscription, checkpoint_id)
        history = read_history_prefix(connection, subscription, checkpoint.sequence)

    # Stable node ids use the precise revision reference; edges use the same ids.
    nodes = {}
    edges = []
    queried_id = None

    def remember(node) -> tuple:
        node_id = (node['ref']['station'], node['ref']['day'], node['ref']['revision'])
        nodes[node_id] = node
        return node_id

    for posted in history:
        if posted.move_order is not None:
            move = posted.move_order
            source_id = (*move.source.key, move.source.revision)
            if source_id not in nodes:
                raise RuntimeError(
                    f'history for {subscription!r} is missing move source {move.source.key!r}')
            target_node = {
                'batch_id': posted.batch_id, 'sequence': posted.sequence,
                'ref': move.target.as_dict(), 'kind': 'move',
                'state': move.state, 'liters': move.liters,
                'origin': {'document': move.origin.document, 'row': move.origin.row},
            }
            target_id = remember(target_node)
            edges.append((source_id, target_id))
            if _ref_matches(move.source, station, day, revision):
                queried_id = source_id
            if _ref_matches(move.target, station, day, revision):
                queried_id = target_id
            continue
        for delta in posted.deltas:
            after = delta.after
            entry = {
                'batch_id': posted.batch_id, 'sequence': posted.sequence,
                'ref': _ref_dict(after),
                'kind': 'move' if after.mode == 'move' else after.mode,
                'state': after.state,
                'liters': None if after.liters is None else after.liters,
                'origin': {'document': after.origin.document, 'row': after.origin.row},
            }
            node_id = remember(entry)
            if delta.before is not None:
                before_id = (*delta.before.key, delta.before.revision)
                if before_id not in nodes:
                    raise RuntimeError(
                        f'history for {subscription!r} is missing predecessor '
                        f'{delta.before.key!r}')
                edges.append((before_id, node_id))
            if _ref_matches(after, station, day, revision):
                queried_id = node_id

    if queried_id is None:
        raise KeyError(
            f'revision {(station, day, revision)!r} is not visible at checkpoint '
            f'{checkpoint_id!r} for subscription {subscription!r}')

    successors = dict(edges)
    predecessors = {target: source for source, target in edges}
    if len(predecessors) != len(edges):
        raise RuntimeError('revision history contains a converging chain')

    current_id = queried_id
    guard = set()
    while current_id in successors:
        if current_id in guard:
            raise RuntimeError('revision history contains a cycle')
        guard.add(current_id)
        current_id = successors[current_id]

    chain_ids = []
    cursor = current_id
    guard = set()
    while cursor is not None:
        if cursor in guard:
            raise RuntimeError('revision history contains a cycle')
        guard.add(cursor)
        chain_ids.append(cursor)
        cursor = predecessors.get(cursor)

    public_keys = ('batch_id', 'sequence', 'ref', 'kind', 'state', 'liters', 'origin')
    chain = [{key: nodes[node_id][key] for key in public_keys}
             for node_id in reversed(chain_ids)]
    return {'current': dict(nodes[current_id]['ref']), 'history': chain}
