import json
from pathlib import Path
from tempfile import TemporaryDirectory

from move_fixture import cases, prepare
from warehouse.fork.handoff import fork, fork_status
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read, trace
from warehouse.ledger.post import post
from warehouse.relay.stream import relay_transaction
from warehouse.retention.reclaim import reclaim
from warehouse.source.publish import publish


def lose_admission_reply(boundary):
    if boundary == 'after_fork_admission':
        raise TimeoutError('值班台未收到中继受理回包')


def run(relay, ledger, batches):
    main = initialize(relay, ledger)
    original = publish(relay, batches[0])
    first = post(relay, ledger, main).checkpoint
    waiting = fork(relay, ledger, 'R', main, first.checkpoint_id, boundary=lose_admission_reply)
    assert waiting.status == 'waiting' and waiting.receipt is None, 'FORK_CONTINUITY: waiting receipt'
    later = [publish(relay, batch) for batch in batches[1:]]
    main_totals = [first.total_liters]
    main_totals.extend(post(relay, ledger, main).checkpoint.total_liters for _ in later)
    before = reclaim(relay, 3)
    assert before.deleted == ('B1',) and before.protected == ('B2', 'B3'), 'FORK_CONTINUITY: retention'

    initialize(relay, ledger)
    ready = fork(relay, ledger, 'R', main, first.checkpoint_id)
    assert ready.status == 'ready', 'FORK_CONTINUITY: recovery'
    review = ready.receipt.review_subscription
    start = read(ledger, review, ready.receipt.initial_checkpoint)
    totals = [start.total_liters]
    evidence = []
    for _ in later:
        result = post(relay, ledger, review)
        totals.append(result.checkpoint.total_liters)
        delta = result.batch.deltas[0]
        evidence.append([result.batch.batch_id, delta.liters_delta, delta.before.origin.document,
                         delta.after.origin.document, delta.after.origin.row])
    after = reclaim(relay, 3)
    initialize(relay, ledger)
    with relay_transaction(relay, write=False) as connection:
        remaining = connection.execute('SELECT batch_id FROM relay_entries ORDER BY sequence').fetchall()
    final = read(ledger, review, 'C3')
    actual = {
        'published_deltas': [batch.deltas[0].liters_delta for batch in [original, *later]],
        'held_during_handoff': list(before.protected),
    }
    downstream = {
        'main_totals': main_totals, 'review_totals': totals, 'review_evidence': evidence,
        'released_after_confirmation': list(after.deleted), 'remaining_work': remaining,
        'historical_batch_ids': [batch.batch_id for batch in final.history],
        'same_receipt_on_reopen': fork_status(ledger, 'R') == ready,
    }
    expected = {'published_deltas': [7, 11, -4], 'held_during_handoff': ['B2', 'B3']}
    downstream_expected = {
        'main_totals': [7, 18, 14], 'review_totals': [7, 18, 14],
        'review_evidence': [['B2', 11, 'meter-B1', 'meter-B2', 2],
                            ['B3', -4, 'meter-B2', 'meter-B3', 3]],
        'released_after_confirmation': ['B2', 'B3'], 'remaining_work': [],
        'historical_batch_ids': ['B1', 'B2', 'B3'], 'same_receipt_on_reopen': True,
    }
    assert actual == expected, f'FORK_CONTINUITY: {actual!r}'
    assert downstream == downstream_expected, f'FORK_CONTINUITY: {downstream!r}'
    print(f'主订阅逐批账量 {main_totals} L，复核订阅逐批账量 {totals} L。')
    print(f'交班时保留 {before.protected}，复核确认后实删 {after.deleted}。')
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'v6',
        'input': {'batches': batches, 'request': 'R', 'checkpoint': 'C1',
                  'reply_lost_at': 'after_fork_admission'},
        'expected': expected, 'actual': actual,
        'downstream_expected': downstream_expected, 'downstream_actual': downstream,
        'code_paths': ['warehouse/source/publish.py', 'warehouse/relay/admit.py',
                       'warehouse/retention/reclaim.py', 'warehouse/fork/recovery.py',
                       'warehouse/ledger/post.py'],
        'oracle': '按输入手算 7−0、18−7、14−18。复核确认前两条原批应保留，确认后查工作表实删。',
    }, ensure_ascii=False, separators=(',', ':')))


def run_move(relay, ledger):
    """Station-day move: transfer, refill the old site, correct the new site,
    then trace both readings back through their full evidence chains."""
    fixture = cases()
    main, opening = prepare(relay, ledger)
    assert opening.total_liters == 14, 'MOVE_REVIEW: opening must hold 14 L'

    move_batch = publish(relay, fixture['move'])
    move_post = post(relay, ledger, main)
    c2 = read(ledger, main, move_post.checkpoint.checkpoint_id)
    assert c2.total_liters == 14, 'MOVE_REVIEW: total liters must survive the move'
    assert [b.key for b in c2.checkpoint.balances] == [
        ('B', '2026-01-15'), ('C', '2026-01-16')], 'MOVE_REVIEW: source must be vacated'

    refill_batch = publish(relay, fixture['refill'])
    post(relay, ledger, main)
    correction_batch = publish(relay, fixture['correction'])
    post(relay, ledger, main)
    c4 = read(ledger, main, 'C4')
    assert c4.total_liters == 21, 'MOVE_REVIEW: refill and correction must reconcile to 21 L'

    # Full-handover review starts at C2 and independently catches B3/B4.
    handoff = fork(relay, ledger, 'MR', main, 'C2')
    assert handoff.status == 'ready', 'MOVE_REVIEW: review handoff must be ready'
    review = handoff.receipt.review_subscription
    post(relay, ledger, review)
    post(relay, ledger, review)
    assert read(ledger, review, 'C4').total_liters == 21, 'MOVE_REVIEW: review total'

    moved_chain = trace(ledger, main, 'C4', 'A', '2026-01-15', 'a1')
    refill_chain = trace(ledger, main, 'C4', 'A', '2026-01-15', 'a2')
    expected_moved = {
        'current': {'station': 'C', 'day': '2026-01-16', 'revision': 'c2'},
        'history': [
            {'batch_id': 'B1', 'ref': {'station': 'A', 'day': '2026-01-15', 'revision': 'a1'},
             'kind': 'initial', 'origin': {'document': 'field-a1', 'row': 1}},
            {'batch_id': 'B2', 'ref': {'station': 'C', 'day': '2026-01-16', 'revision': 'c1'},
             'kind': 'move', 'origin': {'document': 'move-slip-14', 'row': 3}},
            {'batch_id': 'B4', 'ref': {'station': 'C', 'day': '2026-01-16', 'revision': 'c2'},
             'kind': 'correction', 'origin': {'document': 'field-c2', 'row': 1}},
        ],
    }
    assert [{k: item[k] for k in ('batch_id', 'ref', 'kind', 'origin')}
            for item in moved_chain['history']] == expected_moved['history']
    assert moved_chain['current'] == expected_moved['current']
    assert [item['ref']['revision'] for item in refill_chain['history']] == ['a2']
    # The review subscription traces the same chains from its own checkpoint.
    assert trace(ledger, review, 'C4', 'A', '2026-01-15', 'a1') == moved_chain

    # Once both subscriptions confirm, relay work records can be reclaimed;
    # the ledger alone must still answer every historical trace.
    reclaim(relay, 4)
    with relay_transaction(relay, write=False) as connection:
        remaining = connection.execute('SELECT COUNT(*) FROM relay_entries').fetchone()[0]
    assert remaining == 0, 'MOVE_REVIEW: all work records must be releasable'
    isolated = trace(ledger, main, 'C4', 'C', '2026-01-16', 'c1')
    assert [item['ref']['revision'] for item in isolated['history']] == ['a1', 'c1', 'c2']
    assert trace(ledger, main, 'C2', 'A', '2026-01-15', 'a1')['current'] == {
        'station': 'C', 'day': '2026-01-16', 'revision': 'c1'}

    print(f'改列批 {move_batch.batch_id}（流序 {move_batch.sequence}）把 A/a1 改列至 C/c1，'
          f'原址再报 {refill_batch.batch_id} 的 a2，新址更正 {correction_batch.batch_id} 的 c2。')
    print('两笔读数在 C4 分别核回：'
          f"{[item['ref']['revision'] for item in moved_chain['history']]} 与 "
          f"{[item['ref']['revision'] for item in refill_chain['history']]}，合计 {c4.total_liters} L。")
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'move-review',
        'input': {'opening': fixture['opening'], 'move': fixture['move'],
                  'refill': fixture['refill'], 'correction': fixture['correction'],
                  'request': 'MR', 'checkpoint': 'C2'},
        'expected': {'totals': {'C1': 14, 'C2': 14, 'C3': 19, 'C4': 21},
                     'moved_chain_revisions': ['a1', 'c1', 'c2'],
                     'refill_chain_revisions': ['a2']},
        'actual': {'totals': {cp: read(ledger, main, cp).total_liters
                              for cp in ('C1', 'C2', 'C3', 'C4')},
                   'moved_chain_revisions': [item['ref']['revision']
                                             for item in moved_chain['history']],
                   'refill_chain_revisions': [item['ref']['revision']
                                              for item in refill_chain['history']],
                   'review_total': read(ledger, review, 'C4').total_liters,
                   'relay_entries_left': remaining},
        'code_paths': ['warehouse/source/publish.py', 'warehouse/source/moves.py',
                       'warehouse/ledger/post.py', 'warehouse/ledger/checkpoints.py',
                       'warehouse/fork/handoff.py', 'warehouse/retention/reclaim.py'],
        'oracle': '改列保持 14 L 总量并腾空原址；原址首报 +5 L 得 19 L；新址 7→9 更正 +2 L 得 21 L。',
    }, ensure_ascii=False, separators=(',', ':')))


def main():
    sample = Path(__file__).with_name('sample-batches.json')
    batches = json.loads(sample.read_text(encoding='utf-8'))
    with TemporaryDirectory(prefix='chenghe-metering-') as directory:
        workspace = Path(directory)
        run(workspace / 'relay.sqlite', workspace / 'ledger.sqlite', batches)
        run_move(workspace / 'move-relay.sqlite', workspace / 'move-ledger.sqlite')


if __name__ == '__main__':
    main()
