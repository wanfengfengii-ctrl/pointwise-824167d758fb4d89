import json
from dataclasses import asdict, dataclass
from sqlite3 import Connection

from warehouse.source.deltas import DeltaEntry


@dataclass(frozen=True)
class StationDayBalance:
    station: str
    day: str
    revision: str
    state: str
    liters: int | None

    @property
    def key(self) -> tuple[str, str]:
        return self.station, self.day

    @property
    def contribution(self) -> int:
        return 0 if self.liters is None else self.liters


def initialize_balances(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS balances (
            subscription TEXT NOT NULL, station TEXT NOT NULL, day TEXT NOT NULL,
            revision TEXT NOT NULL, state TEXT NOT NULL, liters TEXT,
            PRIMARY KEY (subscription, station, day),
            FOREIGN KEY (subscription) REFERENCES subscriptions (subscription),
            CHECK ((state = 'missing' AND liters IS NULL) OR
                   (state = 'reading' AND liters IS NOT NULL))
        )
    ''')


def load_balances(connection: Connection, subscription: str) -> tuple[StationDayBalance, ...]:
    rows = connection.execute('''
        SELECT station, day, revision, state, liters FROM balances
        WHERE subscription = ? ORDER BY station, day
    ''', (subscription,))
    return tuple(StationDayBalance(station, day, revision, state,
                                   None if liters is None else int(liters))
                 for station, day, revision, state, liters in rows)


def apply_deltas(balances: tuple[StationDayBalance, ...],
                 deltas: tuple[DeltaEntry, ...]) -> tuple[StationDayBalance, ...]:
    by_key = {balance.key: balance for balance in balances}
    for entry in deltas:
        after = entry.after
        before = entry.before
        current = by_key.get(after.key)
        if before is None:
            if current is not None:
                raise ValueError(f'unexpected first report for {after.key!r}')
            old_liters = 0
        else:
            if current is None or (current.revision, current.state, current.liters) != (
                    before.revision, before.state, before.liters):
                raise ValueError(f'delta predecessor does not match the balance for {after.key!r}')
            old_liters = current.contribution
        new_liters = old_liters + entry.liters_delta
        if new_liters != after.contribution:
            raise ValueError(f'delta does not reconcile with the reported liters for {after.key!r}')
        by_key[after.key] = StationDayBalance(*after.key, after.revision, after.state,
                                              None if after.state == 'missing' else new_liters)
    return tuple(by_key[key] for key in sorted(by_key))


def save_balances(connection: Connection, subscription: str,
                  balances: tuple[StationDayBalance, ...]) -> None:
    connection.executemany('''
        INSERT INTO balances (subscription, station, day, revision, state, liters)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (subscription, station, day) DO UPDATE SET
            revision = excluded.revision, state = excluded.state, liters = excluded.liters
    ''', [(subscription, *value.key, value.revision, value.state,
           None if value.liters is None else str(value.liters)) for value in balances])


def encode_balances(balances: tuple[StationDayBalance, ...]) -> str:
    return json.dumps([asdict(item) for item in balances], separators=(',', ':'))


def decode_balances(body: str) -> tuple[StationDayBalance, ...]:
    return tuple(StationDayBalance(**item) for item in json.loads(body))
