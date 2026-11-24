from tests.support import WorkspaceTestCase, fail_at
from warehouse.fork.handoff import fork, fork_status
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.relay.stream import read_stream_bounds, relay_transaction
from warehouse.retention.reclaim import reclaim
from warehouse.source.publish import publish


class RetentionTests(WorkspaceTestCase):
    def test_release_after_both_ack(self):
        self.publish_sample(1, 2, 3)
        self.run_post()
        started = fork(self.relay, self.ledger, 'R', self.main, 'C1')
        review = started.receipt.review_subscription
        self.run_post()
        self.run_post()
        early = reclaim(self.relay, 3)
        self.assertEqual((early.deleted, early.protected), (('B1',), ('B2', 'B3')))
        self.assertEqual(self.work_ids(), ('B2', 'B3'))
        self.run_post(review)
        self.run_post(review)
        final = reclaim(self.relay, 3)
        self.assertEqual((final.reclaimed_through, final.deleted, final.protected),
                         (3, ('B2', 'B3'), ()))
        initialize(self.relay, self.ledger)
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM relay_entries').fetchone()[0], 0)
        for subscription in (self.main, review):
            view = read(self.ledger, subscription, 'C3')
            self.assertEqual(view.total_liters, 14)
            self.assertEqual([p.batch_id for p in view.history], ['B1', 'B2', 'B3'])
            self.assertEqual(view.history[-1].deltas[0].after.origin.document, 'meter-B3')
        self.assertEqual(fork_status(self.ledger, 'R'), started)
        self.assertEqual(self.publish_sample(2)[0].sequence, 2)
        self.assertEqual(self.work_ids(), ())

    def test_physical_release(self):
        self.publish_sample(1, 2, 3)
        self.run_post()
        first = fork(self.relay, self.ledger, 'R', self.main, 'C1').receipt.review_subscription
        second = fork(self.relay, self.ledger, 'S', self.main, 'C1').receipt.review_subscription
        for subscription in (self.main, first):
            self.run_post(subscription)
            self.run_post(subscription)
        self.assertEqual(reclaim(self.relay).protected, ('B2', 'B3'))
        self.run_post(second)
        self.assertEqual(reclaim(self.relay).deleted, ('B2',))
        self.assertEqual(self.work_ids(), ('B3',))
        self.run_post(second)
        self.assertEqual(reclaim(self.relay).deleted, ('B3',))
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(connection.execute('SELECT sequence FROM relay_entries').fetchall(), [])
            self.assertEqual(read_stream_bounds(connection).reclaimed_through, 3)
        self.assertEqual(reclaim(self.relay).deleted, ())

    def test_reclaim_rollback(self):
        self.publish_sample(1, 2)
        self.run_post()
        self.run_post()
        with self.assertRaisesRegex(TimeoutError, 'before_reclaim_commit'):
            reclaim(self.relay, 2, boundary=fail_at('before_reclaim_commit'))
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(read_stream_bounds(connection).reclaimed_through, 0)
            rows = connection.execute('SELECT batch_id FROM relay_entries ORDER BY sequence')
            self.assertEqual(rows.fetchall(), [('B1',), ('B2',)])
        self.assertEqual(reclaim(self.relay, 2).deleted, ('B1', 'B2'))
        self.assertEqual(read(self.ledger, self.main, 'C2').total_liters, 18)

    def test_initialize_again(self):
        self.publish_sample(1, 2, 3)
        self.run_post()
        pending = fork(self.relay, self.ledger, 'R', self.main, 'C1',
                       boundary=fail_at('after_fork_admission'))
        self.assertEqual(pending.status, 'waiting')
        self.run_post()
        self.assertEqual(reclaim(self.relay, 2).deleted, ('B1',))
        self.assertEqual(initialize(self.relay, self.ledger), self.main)
        self.assertEqual(fork_status(self.ledger, 'R'), pending)
        self.assertEqual(self.confirmed(), 2)
        self.assertEqual(self.work_ids(), ('B2', 'B3'))
        with relay_transaction(self.relay, write=False) as connection:
            bounds = read_stream_bounds(connection)
            self.assertEqual((bounds.published_through, bounds.reclaimed_through), (3, 1))
        self.assertEqual(self.run_post().batch.batch_id, 'B3')
        ready = fork(self.relay, self.ledger, 'R', self.main, 'C1')
        self.assertEqual(ready.status, 'ready')
        initialize(self.relay, self.ledger)
        self.assertEqual(fork_status(self.ledger, 'R'), ready)
        with self.assertRaisesRegex(ValueError, 'already belongs'):
            initialize(self.relay, self.ledger, primary='other')
        self.assertEqual(self.confirmed(), 3)

    def test_initialize_resumes_between_databases(self):
        relay = self.root / 'partial-relay.sqlite'
        ledger = self.root / 'partial-ledger.sqlite'
        with self.assertRaisesRegex(TimeoutError, 'after_initialize_relay'):
            initialize(relay, ledger, primary='night', boundary=fail_at('after_initialize_relay'))
        self.assertFalse(ledger.exists())
        published = publish(relay, self.sample[0])
        self.assertEqual(initialize(relay, ledger, primary='night'), 'night')
        self.assertEqual(read(ledger, 'night', 'C0').total_liters, 0)
        self.assertEqual(publish(relay, self.sample[0]), published)
        with self.assertRaisesRegex(ValueError, 'distinct'):
            initialize(relay, relay)
