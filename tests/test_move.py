import json

from tests.support import WorkspaceTestCase, fail_at, revision
from warehouse.fork.handoff import fork
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read, trace
from warehouse.ledger.store import ledger_transaction
from warehouse.ledger.subscriptions import find_subscription
from warehouse.relay.stream import read_stream_bounds, relay_transaction
from warehouse.retention.reclaim import reclaim
from warehouse.source.batches import find_published_batch
from warehouse.source.publish import publish
from move_fixture import cases, prepare


class MovePublishTests(WorkspaceTestCase):
    def move_case(self, **overrides):
        batch = json.loads(json.dumps(cases()['move']))
        if overrides:
            batch['move'].update(overrides)
        return batch

    def test_move_publish_shape_and_replay(self):
        prepare(self.relay, self.ledger)
        first = publish(self.relay, cases()['move'])
        self.assertEqual((first.batch_id, first.sequence), ('B2', 2))
        self.assertEqual(first.deltas, ())
        self.assertEqual(first.move, cases()['move']['move'])
        again = publish(self.relay, cases()['move'])
        self.assertEqual(again, first)
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(read_stream_bounds(connection).published_through, 2)
            archived = find_published_batch(connection, 'B2')[0]
            self.assertEqual(archived.move, cases()['move']['move'])

    def test_stale_source_rejected_without_sequence(self):
        prepare(self.relay, self.ledger)
        stale = self.move_case(**{'from': {'station': 'A', 'day': '2026-01-15', 'revision': 'old'}})
        with self.assertRaisesRegex(ValueError, 'current revision'):
            publish(self.relay, stale)
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(read_stream_bounds(connection).published_through, 1)
            self.assertIsNone(find_published_batch(connection, 'B2'))
            self.assertEqual(connection.execute(
                'SELECT COUNT(*) FROM station_move_events').fetchone()[0], 0)

    def test_same_station_day_rejected(self):
        prepare(self.relay, self.ledger)
        bad = self.move_case(**{'to': {'station': 'A', 'day': '2026-01-15', 'revision': 'a9'}})
        with self.assertRaisesRegex(ValueError, 'different station-days'):
            publish(self.relay, bad)

    def test_occupied_target_rejected(self):
        prepare(self.relay, self.ledger)
        bad = self.move_case(**{'to': {'station': 'B', 'day': '2026-01-15', 'revision': 'b9'}})
        with self.assertRaisesRegex(ValueError, 'occupied'):
            publish(self.relay, bad)

    def test_revision_id_reuse_rejected(self):
        prepare(self.relay, self.ledger)
        publish(self.relay, cases()['move'])
        self.run_post()
        reuse = {'batch_id': 'B5', 'move': {
            'from': {'station': 'B', 'day': '2026-01-15', 'revision': 'b1'},
            'to': {'station': 'C', 'day': '2026-01-16', 'revision': 'c1'},
            'origin': {'document': 'x', 'row': 1}}}
        with self.assertRaisesRegex(ValueError, 'already used'):
            publish(self.relay, reuse)

    def test_same_batch_id_different_content_rejected(self):
        prepare(self.relay, self.ledger)
        publish(self.relay, cases()['move'])
        conflict = self.move_case(origin={'document': 'forged', 'row': 9})
        conflict['batch_id'] = 'B2'
        with self.assertRaisesRegex(ValueError, 'different revisions'):
            publish(self.relay, conflict)

    def test_invalid_move_shapes(self):
        prepare(self.relay, self.ledger)
        with self.assertRaisesRegex(ValueError, 'batch requires'):
            publish(self.relay, {'batch_id': 'X'})
        bad_ref = self.move_case(**{'from': {'station': 'A', 'day': '2026-01-15'}})
        with self.assertRaisesRegex(ValueError, 'from'):
            publish(self.relay, bad_ref)
        bad_day = self.move_case(**{'to': {'station': 'C', 'day': '16-01-2026', 'revision': 'c1'}})
        with self.assertRaisesRegex(ValueError, 'isoformat'):
            publish(self.relay, bad_day)
        bad_origin = self.move_case(origin={'document': 'm', 'row': 0})
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            publish(self.relay, bad_origin)
        with self.assertRaisesRegex(ValueError, 'invalid revision mode'):
            publish(self.relay, {'batch_id': 'sneak', 'revisions': [
                revision('z1', 1, **{'mode': 'move'})]})

    def test_publish_rollback_leaves_no_partial_move(self):
        prepare(self.relay, self.ledger)
        with self.assertRaisesRegex(TimeoutError, 'before_publish_commit'):
            publish(self.relay, cases()['move'], boundary=fail_at('before_publish_commit'))
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(read_stream_bounds(connection).published_through, 1)
            self.assertEqual(self.work_ids(), ('B1',))
            self.assertEqual(connection.execute(
                'SELECT COUNT(*) FROM station_move_events').fetchone()[0], 0)
        self.assertEqual(publish(self.relay, cases()['move']).sequence, 2)


class MoveLedgerTests(WorkspaceTestCase):
    def _open_full_case(self):
        main, _ = prepare(self.relay, self.ledger)
        fixture = cases()
        publish(self.relay, fixture['move'])
        self.run_post()
        publish(self.relay, fixture['refill'])
        self.run_post()
        publish(self.relay, fixture['correction'])
        self.run_post()
        return main, fixture

    def test_balances_and_totals_per_checkpoint(self):
        main, _ = self._open_full_case()
        expected = {
            'C1': ([('A', '2026-01-15', 'a1', 7), ('B', '2026-01-15', 'b1', 7)], 14),
            'C2': ([('B', '2026-01-15', 'b1', 7), ('C', '2026-01-16', 'c1', 7)], 14),
            'C3': ([('A', '2026-01-15', 'a2', 5), ('B', '2026-01-15', 'b1', 7),
                    ('C', '2026-01-16', 'c1', 7)], 19),
            'C4': ([('A', '2026-01-15', 'a2', 5), ('B', '2026-01-15', 'b1', 7),
                    ('C', '2026-01-16', 'c2', 9)], 21),
        }
        for checkpoint_id, (balances, total) in expected.items():
            view = read(self.ledger, main, checkpoint_id)
            actual = [(b.station, b.day, b.revision, b.liters)
                      for b in view.checkpoint.balances]
            self.assertEqual(actual, balances)
            self.assertEqual(view.total_liters, total)
            self.assertEqual(view.checkpoint.last_batch_id,
                             {'C1': 'B1', 'C2': 'B2', 'C3': 'B3', 'C4': 'B4'}[checkpoint_id])

    def test_move_occupies_one_checkpoint_and_history_entry(self):
        main, _ = self._open_full_case()
        view = read(self.ledger, main, 'C4')
        self.assertEqual([batch.batch_id for batch in view.history],
                         ['B1', 'B2', 'B3', 'B4'])
        move_batch = view.history[1]
        self.assertEqual((move_batch.sequence, move_batch.deltas, move_batch.move_order.target.revision),
                         (2, (), 'c1'))

    def test_trace_chains(self):
        main, _ = self._open_full_case()
        c2_a1 = trace(self.ledger, main, 'C2', 'A', '2026-01-15', 'a1')
        self.assertEqual(c2_a1['current'],
                         {'station': 'C', 'day': '2026-01-16', 'revision': 'c1'})
        self.assertEqual([(n['ref']['revision'], n['kind']) for n in c2_a1['history']],
                         [('a1', 'initial'), ('c1', 'move')])
        self.assertEqual([n['origin'] for n in c2_a1['history']],
                         [{'document': 'field-a1', 'row': 1},
                          {'document': 'move-slip-14', 'row': 3}])
        c2_c1 = trace(self.ledger, main, 'C2', 'C', '2026-01-16', 'c1')
        self.assertEqual(c2_c1, c2_a1)
        c4_a1 = trace(self.ledger, main, 'C4', 'A', '2026-01-15', 'a1')
        self.assertEqual(c4_a1['current'],
                         {'station': 'C', 'day': '2026-01-16', 'revision': 'c2'})
        self.assertEqual([(n['ref']['revision'], n['kind'], n['liters'])
                          for n in c4_a1['history']],
                         [('a1', 'initial', 7), ('c1', 'move', 7), ('c2', 'correction', 9)])
        c4_a2 = trace(self.ledger, main, 'C4', 'A', '2026-01-15', 'a2')
        self.assertEqual([n['ref']['revision'] for n in c4_a2['history']], ['a2'])
        c4_b1 = trace(self.ledger, main, 'C4', 'B', '2026-01-15', 'b1')
        self.assertEqual([n['ref']['revision'] for n in c4_b1['history']], ['b1'])
        c1_a1 = trace(self.ledger, main, 'C1', 'A', '2026-01-15', 'a1')
        self.assertEqual([n['kind'] for n in c1_a1['history']], ['initial'])
        with self.assertRaisesRegex(KeyError, 'not visible'):
            trace(self.ledger, main, 'C1', 'C', '2026-01-16', 'c1')
        with self.assertRaisesRegex(KeyError, 'unknown checkpoint'):
            trace(self.ledger, main, 'C9', 'A', '2026-01-15', 'a1')
        with self.assertRaisesRegex(KeyError, 'unknown ledger subscription'):
            trace(self.ledger, 'nobody', 'C1', 'A', '2026-01-15', 'a1')

    def test_missing_and_zero_carry_their_state(self):
        main = initialize(self.relay, self.ledger)
        publish(self.relay, {'batch_id': 'B1', 'revisions': [
            revision('a1', 7, station='A', day='2026-01-15'),
            revision('b1', 0, station='B', day='2026-01-15', row=2)]})
        self.run_post()
        publish(self.relay, {'batch_id': 'B2', 'revisions': [
            revision('a2', None, station='A', day='2026-01-15', previous='a1')]})
        self.run_post()
        publish(self.relay, {'batch_id': 'B3', 'move': {
            'from': {'station': 'A', 'day': '2026-01-15', 'revision': 'a2'},
            'to': {'station': 'C', 'day': '2026-01-16', 'revision': 'c1'},
            'origin': {'document': 'm1', 'row': 1}}})
        self.run_post()
        publish(self.relay, {'batch_id': 'B4', 'move': {
            'from': {'station': 'B', 'day': '2026-01-15', 'revision': 'b1'},
            'to': {'station': 'D', 'day': '2026-01-16', 'revision': 'd1'},
            'origin': {'document': 'm2', 'row': 1}}})
        self.run_post()
        view = read(self.ledger, main, 'C4')
        self.assertEqual(view.total_liters, 0)
        self.assertEqual([(b.station, b.revision, b.state, b.liters)
                          for b in view.checkpoint.balances],
                         [('C', 'c1', 'missing', None), ('D', 'd1', 'reading', 0)])
        missing_chain = trace(self.ledger, main, 'C4', 'A', '2026-01-15', 'a2')['history']
        self.assertEqual([(n['kind'], n['state'], n['liters']) for n in missing_chain],
                         [('initial', 'reading', 7), ('correction', 'missing', None),
                          ('move', 'missing', None)])
        zero_chain = trace(self.ledger, main, 'C4', 'D', '2026-01-16', 'd1')['history']
        self.assertEqual([(n['kind'], n['state'], n['liters']) for n in zero_chain],
                         [('initial', 'reading', 0), ('move', 'reading', 0)])

    def test_refill_then_second_move_and_correction_chain(self):
        main, _ = self._open_full_case()
        publish(self.relay, {'batch_id': 'B5', 'move': {
            'from': {'station': 'A', 'day': '2026-01-15', 'revision': 'a2'},
            'to': {'station': 'E', 'day': '2026-01-17', 'revision': 'e1'},
            'origin': {'document': 'm3', 'row': 1}}})
        self.run_post()
        publish(self.relay, {'batch_id': 'B6', 'revisions': [
            revision('e2', 6, station='E', day='2026-01-17', previous='e1')]})
        self.run_post()
        chain = trace(self.ledger, main, 'C6', 'A', '2026-01-15', 'a2')
        self.assertEqual([(n['ref']['station'], n['ref']['revision'], n['kind'])
                          for n in chain['history']],
                         [('A', 'a2', 'initial'), ('E', 'e1', 'move'),
                          ('E', 'e2', 'correction')])
        self.assertEqual(chain['current'],
                         {'station': 'E', 'day': '2026-01-17', 'revision': 'e2'})
        view = read(self.ledger, main, 'C6')
        self.assertEqual(view.total_liters, 22)

    def test_post_rollback_and_lost_ack(self):
        prepare(self.relay, self.ledger)
        publish(self.relay, cases()['move'])
        with self.assertRaisesRegex(TimeoutError, 'before_post_commit'):
            self.run_post(boundary=fail_at('before_post_commit'))
        with self.assertRaisesRegex(KeyError, 'unknown checkpoint'):
            read(self.ledger, self.main, 'C2')
        with self.assertRaisesRegex(TimeoutError, 'after_post_commit'):
            self.run_post(boundary=fail_at('after_post_commit'))
        self.assertEqual(read(self.ledger, self.main, 'C2').total_liters, 14)
        recovered = self.run_post()
        self.assertEqual(recovered.status, 'replayed')
        self.assertTrue(recovered.acknowledgment.advanced)

    def test_review_inherits_move_history_and_traces_prefix(self):
        main, _ = self._open_full_case()
        opened = fork(self.relay, self.ledger, 'R', main, 'C2')
        self.assertEqual(opened.status, 'ready')
        child = opened.receipt.review_subscription
        self.run_post(child)
        self.run_post(child)
        view = read(self.ledger, child, 'C4')
        self.assertEqual(view.total_liters, 21)
        inherited = trace(self.ledger, child, 'C2', 'A', '2026-01-15', 'a1')
        self.assertEqual([n['kind'] for n in inherited['history']], ['initial', 'move'])
        full = trace(self.ledger, child, 'C4', 'A', '2026-01-15', 'a1')
        self.assertEqual([n['ref']['revision'] for n in full['history']],
                         ['a1', 'c1', 'c2'])
        refill_chain = trace(self.ledger, child, 'C4', 'A', '2026-01-15', 'a2')
        self.assertEqual([n['batch_id'] for n in refill_chain['history']], ['B3'])

    def test_trace_survives_reclaim_and_relay_loss(self):
        main, _ = self._open_full_case()
        opened = fork(self.relay, self.ledger, 'R', main, 'C2')
        child = opened.receipt.review_subscription
        # The review still needs B3/B4, so the first reclaim only releases B1/B2.
        self.assertEqual(reclaim(self.relay, 4).deleted, ('B1', 'B2'))
        self.run_post(child)
        self.run_post(child)
        self.assertEqual(reclaim(self.relay, 4).deleted, ('B3', 'B4'))
        self.assertEqual(self.work_ids(), ())
        chain = trace(self.ledger, main, 'C4', 'A', '2026-01-15', 'a1')
        self.assertEqual([n['ref']['revision'] for n in chain['history']],
                         ['a1', 'c1', 'c2'])
        # Old cutoffs stay readable with their own prefix.
        old = trace(self.ledger, main, 'C2', 'C', '2026-01-16', 'c1')
        self.assertEqual([n['ref']['revision'] for n in old['history']], ['a1', 'c1'])
        initialize(self.relay, self.ledger)
        reopened = trace(self.ledger, main, 'C4', 'A', '2026-01-15', 'a1')
        self.assertEqual(reopened, chain)
        with ledger_transaction(self.ledger, write=False) as connection:
            self.assertIsNotNone(find_subscription(connection, main))
