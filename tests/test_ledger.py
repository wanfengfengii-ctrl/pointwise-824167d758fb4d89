from dataclasses import replace

from tests.support import WorkspaceTestCase, fail_at, revision
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import find_subscription
from warehouse.relay.delivery import next_delivery
from warehouse.retention.acks import acknowledge_posted
from warehouse.retention.reclaim import reclaim
from warehouse.source.publish import publish


class LedgerTests(WorkspaceTestCase):
    def test_replay_and_reopen(self):
        self.publish_sample(1, 2, 3)
        first_delivery = next_delivery(self.relay, self.main).entry
        first = self.run_post()
        replay = self.run_post(delivery=first_delivery)
        self.assertEqual((replay.status, replay.checkpoint), ('replayed', first.checkpoint))
        self.assertEqual(self.confirmed(), 1)
        self.assertEqual([self.run_post().checkpoint.total_liters for _ in range(2)], [18, 14])
        self.assertEqual(initialize(self.relay, self.ledger), self.main)
        current = read(self.ledger, self.main, 'C3')
        self.assertEqual(current.total_liters, 14)
        self.assertEqual([batch.batch_id for batch in current.history], ['B1', 'B2', 'B3'])
        self.assertEqual(read(self.ledger, self.main, 'C1').total_liters, 7)

    def test_late_replay(self):
        self.publish_sample(1, 2, 3)
        self.run_post()
        second_delivery = next_delivery(self.relay, self.main).entry
        second = self.run_post()
        self.run_post()
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B1', 'B2', 'B3'))
        initialize(self.relay, self.ledger)
        replay = self.run_post(delivery=second_delivery)
        self.assertEqual((replay.status, replay.checkpoint), ('replayed', second.checkpoint))
        self.assertEqual(replay.acknowledgment.confirmed_through, 3)
        self.assertFalse(replay.acknowledgment.advanced)
        with self.assertRaisesRegex(ValueError, 'original posting'):
            self.run_post(delivery=replace(second_delivery, sequence=3))
        with ledger_transaction(self.ledger, write=False) as connection:
            self.assertEqual(find_subscription(connection, self.main).checkpoint_id, 'C3')
        final = read(self.ledger, self.main, 'C3')
        self.assertEqual([batch.sequence for batch in final.history], [1, 2, 3])
        self.assertEqual(final.total_liters, 14)

    def test_waiting(self):
        initial = read(self.ledger, self.main, 'C0')
        self.assertEqual((initial.total_liters, initial.history), (0, ()))
        result = self.run_post()
        self.assertEqual(result.status, 'waiting')
        self.assertIsNone(result.checkpoint)
        self.publish_sample(1)
        offered = next_delivery(self.relay, self.main)
        self.assertEqual(offered.entry.batch_id, 'B1')
        self.assertEqual(next_delivery(self.relay, self.main), offered)
        self.assertEqual(self.confirmed(), 0)
        self.assertEqual(self.run_post().checkpoint.total_liters, 7)

    def test_historical_prefix(self):
        self.publish_sample(1, 2, 3)
        for _ in range(3):
            self.run_post()
        initialize(self.relay, self.ledger)
        view = read(self.ledger, self.main, 'C2')
        self.assertEqual((view.total_liters, view.checkpoint.last_batch_id), (18, 'B2'))
        self.assertEqual([batch.batch_id for batch in view.history], ['B1', 'B2'])
        self.assertEqual([d.liters_delta for batch in view.history for d in batch.deltas], [7, 11])
        last = view.history[-1].deltas[0]
        self.assertEqual((last.before.origin.document, last.after.origin.document,
                          last.after.origin.row), ('meter-B1', 'meter-B2', 2))

    def test_unknown_read(self):
        with self.assertRaisesRegex(KeyError, 'unknown ledger subscription'):
            read(self.ledger, 'missing', 'C0')
        with self.assertRaisesRegex(KeyError, 'unknown checkpoint'):
            read(self.ledger, self.main, 'C9')
        with self.assertRaisesRegex(KeyError, 'unknown ledger subscription'):
            self.run_post('missing')

    def test_missing_projection(self):
        inputs = [revision('r1', 7), revision('r2', None, previous='r1'),
                  revision('r3', 0, previous='r2')]
        for index, item in enumerate(inputs, start=1):
            publish(self.relay, {'batch_id': f'M{index}', 'revisions': [item]})
            self.run_post()
        views = [read(self.ledger, self.main, f'C{i}') for i in (1, 2, 3)]
        self.assertEqual([view.total_liters for view in views], [7, 0, 0])
        self.assertEqual([(view.checkpoint.balances[0].state, view.checkpoint.balances[0].liters)
                          for view in views], [('reading', 7), ('missing', None), ('reading', 0)])
        self.assertEqual([batch.deltas[0].after.origin.document for batch in views[-1].history],
                         ['field-r1', 'field-r2', 'field-r3'])

    def test_post_rollback(self):
        self.publish_sample(1, 2)
        self.run_post()
        with self.assertRaisesRegex(TimeoutError, 'before_post_commit'):
            self.run_post(boundary=fail_at('before_post_commit'))
        self.assertEqual(read(self.ledger, self.main, 'C1').total_liters, 7)
        with self.assertRaisesRegex(KeyError, 'unknown checkpoint'):
            read(self.ledger, self.main, 'C2')
        with self.assertRaisesRegex(ValueError, 'committed posting'):
            acknowledge_posted(self.relay, self.ledger, self.main, 'B2', 2)
        self.assertEqual(self.confirmed(), 1)
        self.assertEqual(next_delivery(self.relay, self.main).entry.batch_id, 'B2')
        self.assertEqual(reclaim(self.relay, 2).protected, ('B2',))
        self.assertEqual(self.run_post().checkpoint.total_liters, 18)

    def test_ack_after_commit(self):
        self.publish_sample(1, 2)
        self.run_post()
        with self.assertRaisesRegex(TimeoutError, 'after_post_commit'):
            self.run_post(boundary=fail_at('after_post_commit'))
        self.assertEqual(read(self.ledger, self.main, 'C2').total_liters, 18)
        self.assertEqual(self.confirmed(), 1)
        self.assertEqual(reclaim(self.relay, 2).protected, ('B2',))
        initialize(self.relay, self.ledger)
        recovered = self.run_post()
        self.assertEqual((recovered.status, recovered.checkpoint.total_liters), ('replayed', 18))
        self.assertTrue(recovered.acknowledgment.advanced)
        self.assertEqual(self.confirmed(), 2)
        self.assertEqual([b.batch_id for b in read(self.ledger, self.main, 'C2').history], ['B1', 'B2'])
