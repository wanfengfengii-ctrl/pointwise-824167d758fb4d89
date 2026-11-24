# 站日改列与沿革核账

值班员用一张改列单纠正读数的站日归属。改列证据与当时的报送原件要能一起核回。开账、普通报送、订阅和中继回收沿用 README.md。

## 报送改列单

通过现有 `warehouse.source.publish.publish(relay, batch, *, boundary=None)` 入口报送。单据形状如下。

```json
{
  "batch_id": "B2",
  "move": {
    "from": {"station": "A", "day": "2026-01-15", "revision": "a1"},
    "to": {"station": "C", "day": "2026-01-16", "revision": "c1"},
    "origin": {"document": "move-slip-14", "row": 3}
  }
}
```

`from` 和 `to` 都是精确修订引用，字段校验沿用原报送。改列输入和普通 `revisions` 批输入是两种单独的批形状，共用批号和正式流序。两端的 `(station, day)` 须不同。`from` 必须匹配当前有效修订。`to` 必须是当前空置的站日，并使用该站日从未用过的修订号。站日的缺测记录仍占位。曾经使用、目前空置的站日可以接收新修订，过去的修订号仍不能再用。

改列沿用原事实的 `state` 和 `liters`，新修订引用由 `to` 给出，改列原件由 `origin` 给出。原址腾空后可用新修订号作独立首报。目的地接受普通更正，例如对 `c1` 报 `previous="c1"`，能在同一事实沿革中核出这次新更正。

改列成功仍返回 `PublishedBatch`，由 `.batch_id`、`.sequence` 读取原批号和流序，由 `.move` 读取与上面 `move` 同形的字典。按同批号同内容复领首次结果，后续改列、报送和工作行回收均不改变首执。无效单据、过时前件、占位、重复修订号或批号异参抛 `ValueError`。拒绝及提交前失败均不占正式流序，也不留下部分改列。原有 `before_publish_commit` 回调仍适用。

## 入账与核验

继续用 `post` 逐批入账。一个改列批占一个截点。提交后的 `read` 余额中，原址没有条目，目的地有新修订和原计量状态。两端余额及用于追溯的改列证据一起入账。沿用原先的确认与回收顺序。`after_post_commit` 失答时，欠确认的工作行仍可交付，重投复用原截点并补确认。

在 `warehouse.ledger.checkpoints` 新增 `trace(ledger, subscription, checkpoint_id, station, day, revision)`。输入引用可用该订阅在此截点已入账历史中的任一修订，包括已被更正或移走的修订。结果为字典，包含以下两项。

- `current` 为这笔事实在该截点的精确修订引用，字段是 `station`、`day`、`revision`。
- `history` 为截至该截点的完整修订链列表，按正式流序和批内原序排列。每个条目有 `batch_id`、`sequence`、`ref`、`kind`、`state`、`liters`、`origin`。`ref` 是该条目的精确修订引用。`kind` 为 `initial`、`correction` 或 `move`。`origin` 使用 `document` 和 `row`，分别记录该次首报、更正或改列的原件。显式缺测的 `liters` 是 `None`。

未知订阅、截点或当时尚不可见的修订引用抛 `KeyError`。全量接班按自己的订阅及截点查账，继承的前缀也可核回。读取只需要核账库，工作行回收或中继无法访问时结果相同。旧库可按原有 `initialize` 调用重开，已有报送及截点仍可读。

## 交班样例

`move_fixture.cases()` 提供 `opening`、`move`、`refill` 和 `correction` 四份输入。`move_fixture.prepare(relay, ledger)` 在一对新文件中发布并核对 `opening`，返回主订阅号和 C1 账。单独运行 `python3 -B move_fixture.py` 可核验开账的 14 L 及两份 7 L 的独立出处。

| 截点 | 本次账务 | 站日余额 | 合计 |
| --- | --- | --- | --- |
| C1 | 开账 | A/a1 7 L、B/b1 7 L | 14 L |
| C2 | A/a1 改列至 C/c1 | B/b1 7 L、C/c1 7 L | 14 L |
| C3 | A/a2 独立首报 | A/a2 5 L、B/b1 7 L、C/c1 7 L | 19 L |
| C4 | C/c2 更正 C/c1 | A/a2 5 L、B/b1 7 L、C/c2 9 L | 21 L |

C2 上用 a1 或 c1 均查得当前为 c1，沿革为 a1、c1，原件依次为 `field-a1:1` 和 `move-slip-14:3`。C4 上用 a1 查得 a1、c1、c2，末件为 `field-c2:1`。C4 上用 a2 只查得 a2，等值的 b1 也各自独立。C1 上用 a1 仍只查得 a1，用 c1 查询抛 `KeyError`。从 C2 开办全量复核，再实收 B3、B4。两订阅确认且工作行回收后，隔离中继，重开核账库核对同样的沿革。

## 运行约定

根目录 `.pointwise-runtime.json` 继续用版本 1 的 `cli` 清单。`setup` 为准备命令的参数数组列表，可为空。`verify` 和 `demo` 各为一条命令的参数数组。完成后让 `verify` 运行原回归与实际改列核验，业务核验失败时以非零状态退出。让 `demo` 展示实际入账、原址再报、新址更正和断源沿革读回。保留原测试及原演示入口。
