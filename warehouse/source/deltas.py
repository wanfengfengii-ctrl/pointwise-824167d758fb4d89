import json
from dataclasses import dataclass

from warehouse.source.facts import RevisionChange, StationDayRevision, encode_revision, parse_revision


@dataclass(frozen=True)
class DeltaEntry:
    before: StationDayRevision | None
    after: StationDayRevision
    liters_delta: int


def compute_deltas(changes: tuple[RevisionChange, ...]) -> tuple[DeltaEntry, ...]:
    entries = []
    for change in changes:
        old_liters = 0 if change.before is None else change.before.contribution
        entries.append(DeltaEntry(change.before, change.after,
                                  change.after.contribution - old_liters))
    return tuple(entries)


def encode_deltas(entries: tuple[DeltaEntry, ...]) -> str:
    return json.dumps([
        {
            'before': None if entry.before is None else encode_revision(entry.before),
            'after': encode_revision(entry.after),
            'liters_delta': entry.liters_delta,
        }
        for entry in entries
    ], sort_keys=True, separators=(',', ':'))


def decode_deltas(body: str) -> tuple[DeltaEntry, ...]:
    entries = []
    for item in json.loads(body):
        before = None if item['before'] is None else parse_revision(item['before'])
        after = parse_revision(item['after'])
        liters_delta = item['liters_delta']
        if type(liters_delta) is not int:
            raise ValueError('stored delta is not an integer')
        if before is not None and before.key != after.key:
            raise ValueError('stored delta crosses station-days')
        entries.append(DeltaEntry(before, after, liters_delta))
    return tuple(entries)
