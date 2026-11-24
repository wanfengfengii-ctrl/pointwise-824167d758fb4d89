import json
import tempfile
import unittest
from pathlib import Path

from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.post import post
from warehouse.relay.entries import list_entries_through
from warehouse.relay.stream import relay_transaction
from warehouse.retention.claims import find_obligation
from warehouse.source.publish import publish


def fail_at(point):
    def boundary(current):
        if current == point:
            raise TimeoutError(point)
    return boundary


def revision(revision_id, liters, *, station='A', day='2026-01-15', previous=None,
             mode=None, row=1):
    item = {
        'station': station, 'day': day, 'revision': revision_id,
        'mode': mode or ('initial' if previous is None else 'correction'),
        'state': 'missing' if liters is None else 'reading', 'liters': liters,
        'origin': {'document': f'field-{revision_id}', 'row': row},
    }
    if previous is not None:
        item['previous'] = previous
    return item


class WorkspaceTestCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.relay = self.root / 'relay.sqlite'
        self.ledger = self.root / 'ledger.sqlite'
        self.main = initialize(self.relay, self.ledger)
        sample = Path(__file__).resolve().parents[1] / 'sample-batches.json'
        self.sample = json.loads(sample.read_text(encoding='utf-8'))

    def publish_sample(self, *numbers):
        return [publish(self.relay, self.sample[number - 1]) for number in numbers]

    def run_post(self, subscription=None, **kwargs):
        target = self.main if subscription is None else subscription
        return post(self.relay, self.ledger, target, **kwargs)

    def confirmed(self, subscription=None):
        target = self.main if subscription is None else subscription
        with relay_transaction(self.relay, write=False) as connection:
            return find_obligation(connection, target).confirmed_through

    def work_ids(self):
        with relay_transaction(self.relay, write=False) as connection:
            return tuple(entry.batch_id for entry in list_entries_through(connection, 100))
