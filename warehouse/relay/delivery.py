from dataclasses import dataclass

from warehouse.relay.entries import RelayEntry, load_entry
from warehouse.relay.stream import relay_transaction
from warehouse.relay.windows import next_delivery_window


@dataclass(frozen=True)
class DeliveryResult:
    status: str
    entry: RelayEntry | None = None


def next_delivery(relay, subscription: str) -> DeliveryResult:
    with relay_transaction(relay, write=False) as connection:
        window = next_delivery_window(connection, subscription)
        sequence = window.next_sequence
        if sequence is None:
            return DeliveryResult('waiting')
        entry = load_entry(connection, sequence)
        if entry is None:
            raise RuntimeError(f'missing relay record at sequence {sequence}')
        return DeliveryResult('available', entry)
