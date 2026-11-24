# 澄河计量日批数仓

澄河区域供水计量中心按站日核对整升读数。供数岗发布具名日批，夜班核账岗逐批入账，复核岗从主账的指定截点独立接班，库管按各岗的实收进度释放中继空间。最终合计和每次更正的原件都可按号核对。

## 运行

需要 Python 3.11 标准库。工作目录为项目根目录。

```sh
python3 -B check_project.py
python3 -B demo.py
```

`check_project.py` 由运行平台提供，覆盖 `tests` 包中的完整回归。独立检出可用 `python3 -B -m unittest discover -v` 执行同一业务套件。演示在临时目录建立两库，读取 `sample-batches.json`，按手算口径核对实际结果。任何断言失败都会以非零状态退出。

## 一天的核账

1. 值班员建立中继库和核账库。主订阅 `main` 在 `C0`，总量为 0 L。
2. 供数岗发布甲站首报 `B1`。夜班核回 `C1` 的 7 L 及首报原件。
3. 复核员以请求 `R` 从 `C1` 办理接班。中继已受理后若暂未收到结果，可用原号查询或续办。
4. 供数岗发布更正 `B2` 和 `B3`。夜班收到 +11 L、−4 L，分别核得 18 L、14 L。
5. 库管回收到第 3 批。主订阅已确认，尚待复核的两批仍保留，`B1` 可以释放。
6. 复核员从原请求取得独立订阅及首执。复核账从 7 L 起，依原序核对两笔差额及前后原件。
7. 复核确认后再次回收，两条工作行实际删除。两岗仍可按自己的订阅号、截点号核回旧账，复核员可复领首执。
8. 如 `B2` 已先回收才从 `C1` 开办，即使 `B3` 尚在，结果也为 `expired`，原号无成功首执。

## Python 接口

| 模块 | 调用 | 结果 |
| --- | --- | --- |
| `warehouse.ledger.bootstrap` | `initialize(relay, ledger, primary='main')` | 创建或补齐一对库。以后以相同身份重开，已发布、已入账和待办业务保留。 |
| `warehouse.source.publish` | `publish(relay, batch)` | `PublishedBatch` 含原批号、正式流序和有序冻结分录。同号同参复领。 |
| `warehouse.ledger.post` | `post(relay, ledger, subscription)` | `PostResult` 为 `posted`、`replayed` 或 `waiting`。有结果时含截点、原批和确认前沿。 |
| `warehouse.ledger.checkpoints` | `read(ledger, subscription, checkpoint_id)` | `CheckpointView` 含站日账、总量及截至该点的有序历史，包含末批。 |
| `warehouse.fork.handoff` | `fork(relay, ledger, request_id, source_subscription, checkpoint_id)` | `ForkResult` 为 `ready`、`waiting` 或 `expired`。`ready` 带不可变首执。以 `fork_status(ledger, request_id)` 查询已办状态。 |
| `warehouse.retention.reclaim` | `reclaim(relay, through=None)` | `ReclaimResult` 分列实际删除与本次仍受保护的批号。传非负流序作回收上限，省略时以当前流头为上限。 |

库参数为本工作区中两个不同的 SQLite 文件路径。各调用开关自己的连接。`initialize` 返回主订阅号。`post` 可接受关键字 `delivery`，用于把实际领取的旧 `RelayEntry` 重新交给核账。新入账仍须匹配中继当前应交原批，已入账重投按订阅、批号、流序和分录核对。复核订阅继承直到期初截点的历史与旧截点，可在自己的订阅号下追溯。

日批包含 `batch_id` 和非空有序 `revisions`。每项有 `station`、ISO `day`、站日内唯一 `revision`、`mode`、`state`、`liters` 和 `origin`。原件定位由 `document` 和正整数 `row` 组成。首次报送使用 `initial`，站日已有事实才可使用 `correction`。更正可附 `previous` 指定当前有效前件。实读填 `reading` 和非负整数升，显式缺测填 `missing` 和 `null`。缺测撤去旧计量贡献，补报实读零保留实读标签。分录保留完整前后事实和原件，首报的前件为 `null`。序号从 1 连续分配，批内按报送次序逐项处理，站日分别核算，所有加减均使用整数。

非法输入、过时前件或同号异参触发 `ValueError`。未知订阅、截点或查询请求触发 `KeyError`。账务或中继内容破坏连续性时明确报错。暂未发布后继或复制暂未取得终局时返回 `waiting`。调用方可选传 `boundary` 回调观察命名事务边界。回调在提交前抛错触发本库回滚。复制遇到 `TimeoutError` 或 SQLite 忙等候时报告待办，可用相同参数重开；提交后的已落定结果仍可续办或复领。其他数据库错误由调用方处理。

正式批档案用于发布重投和查证，中继投递仅使用工作记录。核账与确认分两库提交，复制受理及保全先由中继提交，复核期初和首执再由核账库提交。中继和核账库之间只交接已提交的事实。工作记录回收与其边界共同提交，正式档案和已入账证据继续保留。
