from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.relay.stream import read_stream_bounds
from warehouse.retention.claims import find_obligation


@dataclass(frozen=True)
class DeliveryWindow:
    confirmed_through: int
    published_through: int

    @property
    def next_sequence(self) -> int | None:
        if self.confirmed_through == self.published_through:
            return None
        return self.confirmed_through + 1


def next_delivery_window(connection: Connection, subscription: str) -> DeliveryWindow:
    obligation = find_obligation(connection, subscription)
    if obligation is None:
        raise KeyError(f'unknown relay subscription {subscription!r}')
    bounds = read_stream_bounds(connection)
    if not bounds.reclaimed_through <= obligation.confirmed_through <= bounds.published_through:
        raise RuntimeError(f'confirmation is outside the relay window for {subscription!r}')
    return DeliveryWindow(obligation.confirmed_through, bounds.published_through)


def admission_window(connection: Connection, sequence: int) -> DeliveryWindow | None:
    bounds = read_stream_bounds(connection)
    if type(sequence) is not int or not 0 <= sequence <= bounds.published_through:
        raise ValueError('specified checkpoint is outside the published stream')
    if sequence < bounds.reclaimed_through:
        return None
    return DeliveryWindow(sequence, bounds.published_through)
