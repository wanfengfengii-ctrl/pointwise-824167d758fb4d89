from pathlib import Path

from warehouse.fork.receipts import initialize_receipts
from warehouse.fork.requests import initialize_requests
from warehouse.ledger.balances import initialize_balances
from warehouse.ledger.checkpoints import initialize_checkpoints
from warehouse.ledger.history import initialize_history
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import ensure_primary_ledger, initialize_subscriptions
from warehouse.relay.admit import initialize_admissions
from warehouse.relay.entries import initialize_entries
from warehouse.relay.stream import initialize_stream, relay_transaction
from warehouse.retention.claims import ensure_primary_obligation, initialize_claims
from warehouse.source.origins import require_text
from warehouse.source.publish import initialize_source


def initialize(relay, ledger, *, primary: str = 'main', boundary=None) -> str:
    primary = require_text(primary, 'primary')
    if primary.startswith('review:'):
        raise ValueError('primary subscription id uses the reserved review prefix')
    relay_path = Path(relay).resolve()
    ledger_path = Path(ledger).resolve()
    if relay_path == ledger_path or (relay_path.exists() and ledger_path.exists() and
                                     relay_path.samefile(ledger_path)):
        raise ValueError('relay and ledger require distinct database files')
    with relay_transaction(relay_path) as connection:
        initialize_stream(connection)
        initialize_source(connection)
        initialize_entries(connection)
        initialize_claims(connection)
        initialize_admissions(connection)
        ensure_primary_obligation(connection, primary)
    if boundary is not None:
        boundary('after_initialize_relay')
    with ledger_transaction(ledger_path) as connection:
        initialize_subscriptions(connection)
        initialize_balances(connection)
        initialize_checkpoints(connection)
        initialize_history(connection)
        initialize_requests(connection)
        initialize_receipts(connection)
        ensure_primary_ledger(connection, primary)
    return primary
