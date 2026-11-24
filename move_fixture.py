from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support import revision
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.ledger.post import post
from warehouse.source.publish import publish


def cases():
    source = {"station": "A", "day": "2026-01-15", "revision": "a1"}
    target = {"station": "C", "day": "2026-01-16", "revision": "c1"}
    return {
        "opening": {"batch_id": "B1", "revisions": [
            revision("a1", 7, station="A", day=source["day"]),
            revision("b1", 7, station="B", day=source["day"], row=2),
        ]},
        "move": {"batch_id": "B2", "move": {
            "from": source, "to": target, "origin": {"document": "move-slip-14", "row": 3},
        }},
        "refill": {"batch_id": "B3", "revisions": [
            revision("a2", 5, station="A", day=source["day"]),
        ]},
        "correction": {"batch_id": "B4", "revisions": [
            revision("c2", 9, station="C", day=target["day"], previous="c1"),
        ]},
    }


def prepare(relay, ledger):
    main = initialize(relay, ledger)
    opening = publish(relay, cases()["opening"])
    posted = post(relay, ledger, main)
    view = read(ledger, main, "C1")
    if (opening.sequence, posted.status, view.total_liters) != (1, "posted", 14):
        raise AssertionError("Opening batch must post at C1 with 14 L")
    balances = [(r.station, r.day, r.revision, r.liters) for r in view.checkpoint.balances]
    if balances != [("A", "2026-01-15", "a1", 7), ("B", "2026-01-15", "b1", 7)]:
        raise AssertionError(f"Unexpected opening balances: {balances!r}")
    sources = [(d.after.origin.document, d.after.origin.row) for d in view.history[0].deltas]
    if sources != [("field-a1", 1), ("field-b1", 2)]:
        raise AssertionError(f"Unexpected opening sources: {sources!r}")
    return main, view


def main():
    with TemporaryDirectory() as directory:
        root = Path(directory)
        _, view = prepare(root / "relay.sqlite", root / "ledger.sqlite")
        for delta in view.history[0].deltas:
            item = delta.after
            print(f"C1 {item.station} {item.day} {item.revision} {item.liters} L "
                  f"({item.origin.document}:{item.origin.row})")
        print(f"C1 total {view.total_liters} L")


if __name__ == "__main__":
    main()
