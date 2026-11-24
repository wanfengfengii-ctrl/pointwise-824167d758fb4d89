import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from sqlite3 import Connection

from warehouse.source.origins import Origin, encode_origin, parse_origin, require_text


@dataclass(frozen=True)
class StationDayRevision:
    station: str
    day: str
    revision: str
    mode: str
    state: str
    liters: int | None
    origin: Origin
    previous: str | None

    @property
    def key(self) -> tuple[str, str]:
        return self.station, self.day

    @property
    def contribution(self) -> int:
        return 0 if self.liters is None else self.liters


@dataclass(frozen=True)
class RevisionChange:
    before: StationDayRevision | None
    after: StationDayRevision


def parse_revision(value) -> StationDayRevision:
    required = {'station', 'day', 'revision', 'mode', 'state', 'liters', 'origin'}
    if not isinstance(value, Mapping) or not required <= value.keys():
        raise ValueError('incomplete station-day revision')
    if set(value) - required - {'previous'}:
        raise ValueError('unknown station-day revision field')
    station = require_text(value['station'], 'station')
    day = require_text(value['day'], 'day')
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError('day must have YYYY-MM-DD form')
    revision = require_text(value['revision'], 'revision')
    previous = value.get('previous')
    if previous is not None:
        previous = require_text(previous, 'previous')
    mode = value['mode']
    if mode not in ('initial', 'correction') or (mode == 'initial' and previous is not None):
        raise ValueError('invalid revision mode or predecessor')
    state = value['state']
    liters = value['liters']
    if state == 'reading':
        if type(liters) is not int or liters < 0:
            raise ValueError('a reading requires nonnegative integer liters')
    elif state != 'missing' or liters is not None:
        raise ValueError('a missing report requires null liters')
    return StationDayRevision(station, day, revision, mode, state, liters,
                              parse_origin(value['origin']), previous)


def encode_revision(revision: StationDayRevision) -> dict:
    return {
        'station': revision.station, 'day': revision.day, 'revision': revision.revision,
        'mode': revision.mode, 'state': revision.state, 'liters': revision.liters,
        'origin': encode_origin(revision.origin), 'previous': revision.previous,
    }


def initialize_facts(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS station_revisions (
            station TEXT NOT NULL, day TEXT NOT NULL, revision TEXT NOT NULL,
            sequence INTEGER NOT NULL, position INTEGER NOT NULL, body TEXT NOT NULL,
            PRIMARY KEY (station, day, revision), UNIQUE (sequence, position)
        )
    ''')
    connection.execute('''
        CREATE INDEX IF NOT EXISTS station_revision_order
        ON station_revisions (station, day, sequence DESC, position DESC)
    ''')


def load_current_revision(connection: Connection, station: str,
                          day: str) -> StationDayRevision | None:
    row = connection.execute('''
        SELECT body FROM station_revisions WHERE station = ? AND day = ?
        ORDER BY sequence DESC, position DESC LIMIT 1
    ''', (station, day)).fetchone()
    return None if row is None else parse_revision(json.loads(row[0]))


def resolve_revisions(connection: Connection,
                      revisions: tuple[StationDayRevision, ...]) -> tuple[RevisionChange, ...]:
    current = {}
    seen = set()
    changes = []
    for revision in revisions:
        key = revision.key
        identity = (*key, revision.revision)
        existing = connection.execute('''
            SELECT 1 FROM station_revisions WHERE station = ? AND day = ? AND revision = ?
        ''', identity).fetchone()
        if identity in seen or existing is not None:
            raise ValueError(f'revision id already used for station-day {key!r}')
        if key not in current:
            current[key] = load_current_revision(connection, *key)
        old = current[key]
        if revision.mode == 'initial':
            if old is not None:
                raise ValueError(f'station-day {key!r} already has a report')
        elif old is None or (revision.previous is not None and revision.previous != old.revision):
            raise ValueError(f'correction does not match the current revision for {key!r}')
        changes.append(RevisionChange(old, revision))
        current[key] = revision
        seen.add(identity)
    return tuple(changes)


def save_revisions(connection: Connection, sequence: int,
                   changes: tuple[RevisionChange, ...]) -> None:
    for position, change in enumerate(changes, start=1):
        after = change.after
        connection.execute('''
            INSERT INTO station_revisions (station, day, revision, sequence, position, body)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (*after.key, after.revision, sequence, position, json.dumps(encode_revision(after))))
