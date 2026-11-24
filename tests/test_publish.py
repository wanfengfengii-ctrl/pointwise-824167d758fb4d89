from copy import deepcopy

from tests.support import WorkspaceTestCase, fail_at, revision
from warehouse.relay.stream import read_stream_bounds, relay_transaction
from warehouse.source.batches import find_published_batch
from warehouse.source.facts import load_current_revision
from warehouse.source.origins import Origin
from warehouse.source.publish import publish


class PublishTests(WorkspaceTestCase):
    def test_signed_deltas(self):
        batches = self.publish_sample(1, 2, 3)
        self.assertEqual([(b.batch_id, b.sequence) for b in batches],
                         [('B1', 1), ('B2', 2), ('B3', 3)])
        entries = [batch.deltas[0] for batch in batches]
        self.assertEqual([entry.liters_delta for entry in entries], [7, 11, -4])
        self.assertIsNone(entries[0].before)
        self.assertEqual([(e.before.liters, e.before.origin) for e in entries[1:]],
                         [(7, Origin('meter-B1', 1)), (18, Origin('meter-B2', 2))])
        self.assertEqual([e.after.origin for e in entries],
                         [Origin('meter-B1', 1), Origin('meter-B2', 2), Origin('meter-B3', 3)])
        self.assertEqual([self.run_post().checkpoint.total_liters for _ in batches], [7, 18, 14])

    def test_missing_and_zero(self):
        inputs = [revision('r1', 7), revision('r2', None, previous='r1'),
                  revision('r3', 0, previous='r2')]
        entries = [publish(self.relay, {'batch_id': f'M{i}', 'revisions': [item]}).deltas[0]
                   for i, item in enumerate(inputs, start=1)]
        self.assertEqual([entry.liters_delta for entry in entries], [7, -7, 0])
        self.assertEqual([(entry.after.state, entry.after.liters) for entry in entries],
                         [('reading', 7), ('missing', None), ('reading', 0)])
        self.assertEqual(entries[2].before.state, 'missing')
        self.assertEqual(entries[2].before.origin, Origin('field-r2', 1))
        self.assertEqual(entries[2].after.origin, Origin('field-r3', 1))

    def test_multi_station_order(self):
        report = {'batch_id': 'mixed', 'revisions': [
            revision('r1', 7), revision('r1', 4, station='B', row=2),
            revision('r2', 10, previous='r1', row=3),
        ]}
        published = publish(self.relay, report)
        self.assertEqual([(e.after.station, e.liters_delta) for e in published.deltas],
                         [('A', 7), ('B', 4), ('A', 3)])
        self.assertEqual(published.deltas[-1].before.origin, Origin('field-r1', 1))
        posted = self.run_post()
        self.assertEqual([(b.station, b.liters) for b in posted.checkpoint.balances],
                         [('A', 10), ('B', 4)])
        report['revisions'][0], report['revisions'][1] = report['revisions'][1], report['revisions'][0]
        with self.assertRaisesRegex(ValueError, 'different revisions'):
            publish(self.relay, report)

    def test_conflicting_retry(self):
        _, original = self.publish_sample(1, 2)
        changed = deepcopy(self.sample[1])
        changed['revisions'][0]['origin']['row'] = 99
        with self.assertRaisesRegex(ValueError, 'different revisions'):
            publish(self.relay, changed)
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(read_stream_bounds(connection).published_through, 2)
            self.assertEqual(find_published_batch(connection, 'B2')[0], original)
            self.assertEqual(load_current_revision(connection, 'A', '2026-01-15').liters, 18)
        self.assertEqual(self.work_ids(), ('B1', 'B2'))

    def test_identical_retry(self):
        first, second, _ = self.publish_sample(1, 2, 3)
        self.assertEqual(self.publish_sample(2)[0], second)
        normalized = deepcopy(self.sample[0])
        normalized['revisions'][0]['previous'] = None
        self.assertEqual(publish(self.relay, normalized), first)
        self.assertEqual(self.work_ids(), ('B1', 'B2', 'B3'))

    def test_invalid_revision(self):
        self.publish_sample(1)
        mixed = {'batch_id': 'two', 'revisions': [revision('s1', 8, station='B'),
                                                revision('r2', 12, previous='stale')]}
        with self.assertRaisesRegex(ValueError, 'current revision'):
            publish(self.relay, mixed)
        with relay_transaction(self.relay, write=False) as connection:
            self.assertIsNone(load_current_revision(connection, 'B', '2026-01-15'))
            self.assertIsNone(find_published_batch(connection, 'two'))
            self.assertEqual(read_stream_bounds(connection).published_through, 1)
        mixed['revisions'][1]['previous'] = 'r1'
        batch = publish(self.relay, mixed)
        self.assertEqual((batch.sequence, [d.liters_delta for d in batch.deltas]), (2, [8, 5]))
        with self.assertRaisesRegex(ValueError, 'already used'):
            publish(self.relay, {'batch_id': 'bad-id', 'revisions': [
                revision('r1', 9, previous='r2'),
            ]})
        with self.assertRaisesRegex(ValueError, 'integer liters'):
            publish(self.relay, {'batch_id': 'bad-value', 'revisions': [revision('r0', True)]})
        self.assertEqual(self.work_ids(), ('B1', 'two'))

    def test_publish_rollback(self):
        with self.assertRaisesRegex(TimeoutError, 'before_publish_commit'):
            publish(self.relay, self.sample[0], boundary=fail_at('before_publish_commit'))
        with relay_transaction(self.relay, write=False) as connection:
            self.assertIsNone(find_published_batch(connection, 'B1'))
            self.assertIsNone(load_current_revision(connection, 'A', '2026-01-15'))
            self.assertEqual(read_stream_bounds(connection).published_through, 0)
        self.assertEqual(self.work_ids(), ())
        self.assertEqual(self.publish_sample(1)[0].sequence, 1)
