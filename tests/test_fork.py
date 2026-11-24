from tests.support import WorkspaceTestCase, fail_at
from warehouse.fork.handoff import fork, fork_status
from warehouse.fork.receipts import find_receipt
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import find_subscription
from warehouse.relay.admit import find_admission
from warehouse.relay.stream import relay_transaction
from warehouse.retention.claims import find_obligation
from warehouse.retention.reclaim import reclaim


class ForkTests(WorkspaceTestCase):
    def open_review(self, checkpoint='C1', request='R', *, boundary=None):
        return fork(self.relay, self.ledger, request, self.main, checkpoint, boundary=boundary)

    def assert_no_success(self, request='R'):
        with ledger_transaction(self.ledger, write=False) as connection:
            self.assertIsNone(find_receipt(connection, request))
            self.assertIsNone(find_subscription(connection, f'review:{request}'))

    def test_independent_progress(self):
        self.publish_sample(1, 2, 3)
        self.assertEqual(self.run_post().checkpoint.total_liters, 7)
        opened = self.open_review()
        self.assertEqual(opened.status, 'ready')
        receipt = opened.receipt
        child = receipt.review_subscription
        start = read(self.ledger, child, receipt.initial_checkpoint)
        self.assertEqual((start.total_liters, [b.batch_id for b in start.history]), (7, ['B1']))
        self.assertEqual(self.run_post().checkpoint.total_liters, 18)
        self.assertEqual((self.confirmed(), self.confirmed(child)), (2, 1))
        self.assertEqual(self.run_post(child).checkpoint.total_liters, 18)
        self.assertEqual(self.run_post(child).checkpoint.total_liters, 14)
        self.assertEqual((self.confirmed(), self.confirmed(child)), (2, 3))
        self.assertEqual(self.run_post().checkpoint.total_liters, 14)
        self.assertEqual(self.open_review(), opened)
        self.assertEqual(fork_status(self.ledger, 'R'), opened)

    def test_conflicting_request(self):
        self.publish_sample(1, 2)
        self.run_post()
        self.run_post()
        original = self.open_review(boundary=fail_at('after_fork_binding'))
        self.assertEqual(original.status, 'waiting')
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.open_review('C2')
        ready = self.open_review()
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.open_review('C2')
        self.assertEqual(fork_status(self.ledger, 'R'), ready)
        self.assertEqual(ready.receipt.source_checkpoint, 'C1')

    def _lost_admission_reply(self, *, early_batch):
        self.publish_sample(1)
        self.run_post()
        if early_batch:
            self.publish_sample(2)
        result = self.open_review(boundary=fail_at('after_fork_admission'))
        self.assertEqual(result.status, 'waiting')
        self.assertEqual(fork_status(self.ledger, 'R').status, 'waiting')
        self.assert_no_success()
        with relay_transaction(self.relay, write=False) as connection:
            admitted = find_admission(connection, 'R')
            self.assertEqual((admitted.status, admitted.sequence, admitted.checkpoint_id),
                             ('admitted', 1, 'C1'))
        if not early_batch:
            self.publish_sample(2)
        self.publish_sample(3)
        self.assertEqual([self.run_post().checkpoint.total_liters for _ in range(2)], [18, 14])
        returned = reclaim(self.relay, 3)
        self.assertEqual((returned.deleted, returned.protected), (('B1',), ('B2', 'B3')),
                         'FORK_CONTINUITY: accepted review must retain B2 and B3')
        self.assertEqual(self.work_ids(), ('B2', 'B3'))
        self.assertEqual(self.confirmed(), 3)
        initialize(self.relay, self.ledger)
        ready = self.open_review()
        self.assertEqual(ready.status, 'ready')
        child = ready.receipt.review_subscription
        start = read(self.ledger, child, ready.receipt.initial_checkpoint)
        self.assertEqual((ready.receipt.sequence, start.total_liters), (1, 7),
                         'FORK_CONTINUITY: review must start at C1 and 7 L')
        records = []
        for _ in range(2):
            posted = self.run_post(child)
            delta = posted.batch.deltas[0]
            records.append((posted.batch.batch_id, delta.liters_delta,
                            delta.before.origin.document, delta.after.origin.document,
                            delta.after.origin.row, posted.checkpoint.total_liters))
        self.assertEqual(records, [('B2', 11, 'meter-B1', 'meter-B2', 2, 18),
                                   ('B3', -4, 'meter-B2', 'meter-B3', 3, 14)])
        self.assertEqual(self.open_review(), ready)
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.open_review('C3')
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B2', 'B3'))
        self.assertEqual(self.work_ids(), ())
        initialize(self.relay, self.ledger)
        for subscription in (self.main, child):
            self.assertEqual([read(self.ledger, subscription, f'C{i}').total_liters
                              for i in (1, 2, 3)], [7, 18, 14])
        self.assertEqual(fork_status(self.ledger, 'R'), ready)

    def test_admitted_reply_lost(self):
        with self.subTest(business='FORK_CONTINUITY'):
            self._lost_admission_reply(early_batch=False)

    def test_window_covers_late_batch(self):
        with self.subTest(business='FORK_CONTINUITY'):
            self._lost_admission_reply(early_batch=True)

    def test_reclaimed_before_admission(self):
        self.publish_sample(1, 2)
        self.run_post()
        self.run_post()
        self.assertEqual(reclaim(self.relay, 2).deleted, ('B1', 'B2'))
        self.assertEqual(self.work_ids(), ())
        expired = self.open_review()
        self.assertEqual((expired.status, expired.receipt), ('expired', None))
        initialize(self.relay, self.ledger)
        self.assertEqual(self.open_review(), expired)
        self.assertEqual(fork_status(self.ledger, 'R'), expired)
        self.assert_no_success()
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(find_admission(connection, 'R').status, 'expired')
            self.assertIsNone(find_obligation(connection, 'review:R'))

    def test_partial_suffix(self):
        self.publish_sample(1, 2, 3)
        self.run_post()
        self.run_post()
        self.assertEqual(reclaim(self.relay, 2).deleted, ('B1', 'B2'))
        self.assertEqual(self.work_ids(), ('B3',))
        self.assertEqual(self.open_review().status, 'expired')
        self.assert_no_success()
        self.assertEqual(self.run_post().checkpoint.total_liters, 14)

    def test_boundary_equal(self):
        self.publish_sample(1, 2)
        self.run_post()
        reclaimed = reclaim(self.relay, 1)
        self.assertEqual((reclaimed.reclaimed_through, reclaimed.deleted), (1, ('B1',)))
        opened = self.open_review()
        self.assertEqual(opened.status, 'ready')
        self.assertEqual(self.run_post(opened.receipt.review_subscription).checkpoint.total_liters, 18)

    def test_admission_rollback(self):
        self.publish_sample(1, 2)
        self.run_post()
        self.assertEqual(self.open_review(boundary=fail_at('before_fork_admission_commit')).status,
                         'waiting')
        with relay_transaction(self.relay, write=False) as connection:
            self.assertIsNone(find_admission(connection, 'R'))
            self.assertIsNone(find_obligation(connection, 'review:R'))
        self.run_post()
        self.assertEqual(reclaim(self.relay, 2).deleted, ('B1', 'B2'))
        self.assertEqual(self.open_review().status, 'expired')
        self.assert_no_success()

    def test_finalize_rollback_and_lost_final_reply(self):
        self.publish_sample(1, 2)
        self.run_post()
        self.assertEqual(self.open_review(boundary=fail_at('before_fork_finalize_commit')).status,
                         'waiting')
        self.assert_no_success()
        self.run_post()
        self.assertEqual(reclaim(self.relay, 2).protected, ('B2',))
        self.assertEqual(self.open_review(boundary=fail_at('after_fork_finalize')).status, 'waiting')
        initialize(self.relay, self.ledger)
        found = fork_status(self.ledger, 'R')
        self.assertEqual(found.status, 'ready')
        self.assertEqual(self.open_review(), found)
        self.assertEqual(read(self.ledger, found.receipt.review_subscription, 'C1').total_liters, 7)
