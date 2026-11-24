from dataclasses import dataclass

from warehouse.relay.entries import delete_entries_through
from warehouse.relay.stream import relay_transaction, set_reclaimed_through
from warehouse.retention.eligibility import plan_reclaim


@dataclass(frozen=True)
class ReclaimResult:
    reclaimed_through: int
    deleted: tuple[str, ...]
    protected: tuple[str, ...]


def reclaim(relay, through: int | None = None, *, boundary=None) -> ReclaimResult:
    with relay_transaction(relay) as connection:
        plan = plan_reclaim(connection, through)
        deleted = delete_entries_through(connection, plan.delete_through)
        if len(deleted) != plan.delete_through - plan.previous_boundary:
            raise RuntimeError('work records do not match the prefix being reclaimed')
        set_reclaimed_through(connection, plan.delete_through)
        if boundary is not None:
            boundary('before_reclaim_commit')
    return ReclaimResult(plan.delete_through, deleted, plan.protected)
