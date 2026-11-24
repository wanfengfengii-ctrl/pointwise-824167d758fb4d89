from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.relay.entries import list_entries_through
from warehouse.relay.stream import read_stream_bounds
from warehouse.retention.claims import list_obligations


@dataclass(frozen=True)
class ReclaimPlan:
    previous_boundary: int
    delete_through: int
    protected: tuple[str, ...]


def plan_reclaim(connection: Connection, through: int | None) -> ReclaimPlan:
    if through is not None and (type(through) is not int or through < 0):
        raise ValueError('reclaim limit must be a nonnegative sequence')
    bounds = read_stream_bounds(connection)
    obligations = list_obligations(connection)
    if not obligations:
        raise RuntimeError('relay has no primary obligation')
    target = bounds.published_through if through is None else min(through, bounds.published_through)
    oldest_confirmation = min(item.confirmed_through for item in obligations)
    if oldest_confirmation < bounds.reclaimed_through:
        raise RuntimeError('a subscription is missing a previously reclaimed prefix')
    delete_through = max(bounds.reclaimed_through, min(target, oldest_confirmation))
    protected = tuple(entry.batch_id for entry in list_entries_through(connection, target)
                      if entry.sequence > delete_through)
    return ReclaimPlan(bounds.reclaimed_through, delete_through, protected)
