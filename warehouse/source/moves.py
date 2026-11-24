import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from sqlite3 import Connection

from warehouse.source.origins import Origin, encode_origin, parse_origin, require_text


@dataclass(frozen=True)
class MoveRef:
    station: str
    day: str
    revision: str

    @property
    def key(self) -> tuple[str, str]:
        return self.station, self.day

    def as_dict(self) -> dict:
        return {'station': self.station, 'day': self.day, 'revision': self.revision}


@dataclass(frozen=True)
class MoveOrder:
    source: MoveRef
    target: MoveRef
    origin: Origin
    state: str
    liters: int | None

    def as_input(self) -> dict:
        return {
            'from': self.source.as_dict(),
            'to': self.target.as_dict(),
            'origin': encode_origin(self.origin),
        }


def parse_move_ref(value, field: str) -> MoveRef:
    if not isinstance(value, Mapping) or set(value) != {'station', 'day', 'revision'}:
        raise ValueError(f'{field} requires station, day and revision')
    station = require_text(value['station'], f'{field}.station')
    day = require_text(value['day'], f'{field}.day')
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError(f'{field}.day must have YYYY-MM-DD form')
    revision = require_text(value['revision'], f'{field}.revision')
    return MoveRef(station, day, revision)


def parse_move(value) -> MoveOrder:
    if not isinstance(value, Mapping) or set(value) != {'from', 'to', 'origin'}:
        raise ValueError('a move batch requires from, to and origin')
    source = parse_move_ref(value['from'], 'move.from')
    target = parse_move_ref(value['to'], 'move.to')
    if source.key == target.key:
        raise ValueError('a move must transfer between different station-days')
    origin = parse_origin(value['origin'])
    # state and liters are filled in from the current source fact at publish time.
    return MoveOrder(source, target, origin, '', None)


def canonical_move_body(value) -> str:
    return json.dumps({
        'from': parse_move_ref(value['from'], 'move.from').as_dict(),
        'to': parse_move_ref(value['to'], 'move.to').as_dict(),
        'origin': encode_origin(parse_origin(value['origin'])),
    }, sort_keys=True, separators=(',', ':'))


def encode_move_payload(move: MoveOrder) -> str:
    return json.dumps({
        'from': move.source.as_dict(),
        'to': move.target.as_dict(),
        'origin': encode_origin(move.origin),
        'state': move.state,
        'liters': move.liters,
    }, sort_keys=True, separators=(',', ':'))


def decode_move_payload(body: str) -> MoveOrder:
    item = json.loads(body)
    source = parse_move_ref(item['from'], 'move.from')
    target = parse_move_ref(item['to'], 'move.to')
    origin = parse_origin(item['origin'])
    state = item['state']
    liters = item['liters']
    if state == 'reading':
        if type(liters) is not int or liters < 0:
            raise ValueError('stored move reading requires nonnegative integer liters')
    elif state != 'missing' or liters is not None:
        raise ValueError('stored move has an invalid state')
    return MoveOrder(source, target, origin, state, liters)


def initialize_move_events(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS station_move_events (
            station TEXT NOT NULL, day TEXT NOT NULL, revision TEXT NOT NULL,
            sequence INTEGER NOT NULL, position INTEGER NOT NULL,
            target_station TEXT NOT NULL, target_day TEXT NOT NULL, target_revision TEXT NOT NULL,
            state TEXT NOT NULL, liters TEXT,
            origin_document TEXT NOT NULL, origin_row INTEGER NOT NULL,
            PRIMARY KEY (station, day, revision),
            UNIQUE (sequence, position),
            UNIQUE (target_station, target_day, target_revision)
        )
    ''')


def save_move_event(connection: Connection, sequence: int, position: int, move: MoveOrder) -> None:
    connection.execute('''
        INSERT INTO station_move_events (
            station, day, revision, sequence, position,
            target_station, target_day, target_revision, state, liters,
            origin_document, origin_row)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (*move.source.key, move.source.revision, sequence, position,
          *move.target.key, move.target.revision, move.state,
          None if move.liters is None else str(move.liters),
          move.origin.document, move.origin.row))


def find_move_event(connection: Connection, station: str, day: str) -> tuple | None:
    return connection.execute('''
        SELECT sequence, position, target_station, target_day, target_revision,
               state, liters, origin_document, origin_row
        FROM station_move_events
        WHERE station = ? AND day = ?
        ORDER BY sequence DESC, position DESC LIMIT 1
    ''', (station, day)).fetchone()


def revision_used_anywhere(connection: Connection, station: str, day: str,
                           revision: str) -> bool:
    # True when the precise revision id was ever attached to this station-day:
    # as a stored report, an outgoing move source, or an incoming move target.
    row = connection.execute('''
        SELECT 1 FROM station_revisions WHERE station = ? AND day = ? AND revision = ?
        UNION ALL
        SELECT 1 FROM station_move_events
        WHERE (station = ? AND day = ? AND revision = ?)
           OR (target_station = ? AND target_day = ? AND target_revision = ?)
        LIMIT 1
    ''', (station, day, revision, station, day, revision,
          station, day, revision)).fetchone()
    return row is not None
