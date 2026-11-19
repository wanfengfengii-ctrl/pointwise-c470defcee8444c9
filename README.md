# 临川水文资料湖

临川水文资料中心接收测站送来的观测读数和分区更正。白班以具名批次入仓，夜班按固定区间更新分区副本，保管员压实小段并退役合格原件。应用使用 Python 3.11 标准库和本地 SQLite。

## 运行

```text
python3 -B check_project.py
python3 -B demo.py
```

`check_project.py` 由控制台物化，运行 `tests` 包下的完整原回归。也可从项目根目录直接运行 `python3 -B -m unittest discover -s tests -t . -v`。演示从 `samples/linchuan.json` 读取固定输入，自动使用测试临时库。示例在东区底账 r=5 后依次提交移出和移回。夜班领取第一页后，保管员发布替换并实际退役原段。重开、追加 43 并续读后，演示将备用副本对到固定的 42 底账，再核对当前 43 的读数。

## Python API

| 正常操作 | 接口 | 返回和约定 |
| --- | --- | --- |
| 打开工作库 | `lake.table.storage.open_database(path, first_commit=None, retention=None)` | 返回需由调用方关闭的连接。新库默认首次提交 1，保留 512 个提交。明确提供的配置须与现存库一致。`initialize_database` 可重复调用。 |
| 入仓 | `lake.ingest.admission.apply(db, BatchRequest(...))` | `BatchRequest(batch_id, edits)` 接受有序 `Edit`，返回 `BatchReceipt(batch_id, commit_no)`。异参重投抛 `BatchConflict`，非法批次抛 `InvalidBatch`。两者均继承 `ValueError`。 |
| 读底账 | `lake.table.snapshot.snapshot(db, snapshot_id='current')` | 返回 `Snapshot(snapshot_id, commit_no, rows)`。用 `register_snapshot(db, snapshot_id)` 为当时的当前底账登记固定编号。已登记名称不可重用。未知快照或读源丢失抛 `SnapshotUnavailable`。 |
| 受理补账 | `lake.feed.subscription.open_feed(db, partition, start, end, page_size)` | 返回持久 `Subscription`，含 `subscription_id` 和 `first_cursor`。形状错误抛 `ValueError`，保留窗口以外或所需提交证据缺失抛 `CoverageError`，早于窗口下界抛其子类 `ExpiredWindow`。同一合法端点的 `(n,n]` 返回空区间。 |
| 领页 | `lake.feed.delivery.read_page(db, subscription_id, token)` | 返回 `FeedPage(changes, next_cursor)`，`done` 表示已到结尾。未知订阅抛 `SubscriptionUnavailable`，错误标记抛 `InvalidCursor`，覆盖失效按上述窗口错误处理。编号和标记在原工作库内使用。 |
| 发布整理 | `lake.compact.publish.compact(db, segment_ids)` | 返回 `PublishedReplacement`，含替换号、新段、原件和发布时可退役列表。通过 `load_catalog(db).segment_ids` 取得活动段号。可以分开调用 `plan_compaction`、`prepare_replacement` 和 `publish_replacement`，按顺序交接该库返回的计划和准备结果。选段非法抛 `ValueError`，迟到计划抛 `StalePlan`，数据库提交错误原样上报。 |
| 退役原件 | `lake.compact.retire.retire(db, replacement_id)` | 返回按原段号排列的逐段结果，状态为 `retired`、`blocked` 或 `already_retired`。`blocked` 附依赖原因，未知替换抛 `ReplacementUnavailable`。 |

`Edit` 和 `BatchRequest` 定义在 `lake.ingest.batch`。`insert` 和 `update` 均携带稳定行号、分区及整数读数，`delete` 只携带行号。名称使用有效的非空白 Unicode 文本并区分原始字面值。`bool` 和浮点数不作为整数读数。读数与上游单位一致，可为负值或零。入仓按同批原序结算，新增要求缺席，更正与撤回要求存在。无净变化或空编辑的成功批次仍有正式回执。错误后缀不会留下前缀变更。

订阅的开端不含、闭端包含。出区或撤回交付 `remove` 和旧行，入区或新增交付 `add` 和新行，留区改值交付 `update` 和新旧行。投影以每次正式提交为边界，按数值提交号和行号文本排序。续页标记绑定本库的订阅编号和上次交付的逻辑位置，重复使用给出相同页。保留窗口按正式提交头推进，压实本身不推进窗口。已覆盖且无变化的提交与缺失证据分别处理。

原段的净贡献用于读快照，原段的有序写入和受影响行的起点用于重放变更。压实依据原提交号合并选中段的最新贡献，撤回标记继续覆盖未选中段的旧行。发布事务同时登记替换、移交保留窗口内的证据，并为能由替换段还原的固定快照迁移读源。退役在独立事务中复核现存依赖，再删除原段载荷及其附属写入。当前底账与固定快照都以稳定行号排列，固定快照仍以原编号和原提交截点可读。公开业务入口自行开启事务，底层读写辅助函数供业务入口在同一事务内组合使用。
