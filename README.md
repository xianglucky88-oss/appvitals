# appvitals — 跨商店监控应用版本更新、评分与评论分页，关联版本发布与口碑变化。

运行栈：Python 3.11+ 标准库 `sqlite3`（离线，无需联网）；有 pytest 时用 pytest，否则 `run_tests.py` 回退标准库 `unittest`。

## 运行方式
读 `fixtures/input.json` → `loader` → `normalizer` → 其余变换 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线 happy-path，输出摄取数量、落库行数与 dedup 决策计数
make test    # 优先 pytest；环境无 pytest 时自动回退 unittest（同一套 TestCase）
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段与事务/重试边界，不在编排层加入业务判断。
业务模块：`versionline`, `review_page`, `score_align`, `store_api`, `unify`, `dedup`。
通用模块：`loader / normalizer / identity / keyer / store / pipeline / cli`。

## dedup：版本/评分/评论的状态化重复判定
`dedup.process(record)` 仍是单记录、无显式状态参数的生产步骤签名，内部由内存状态机 +
sqlite 持久表支撑（`dedup_index / dedup_audit / dedup_quarantine / dedup_meta`）。

### 四类记录的区分（验收口径）
| 场景 | 判定条件 | 决策码 | 落库行为 |
| --- | --- | --- | --- |
| 同一事件重传 | 身份指纹 + 内容指纹 + 墓碑位完全相同 | `retransmit` | 跳过，仅审计计数 |
| 评论/评分编辑 | 身份相同、内容指纹变化、时间戳更新 | `update` | 同一存储键 upsert |
| 评论删除 | `op=delete`/`deleted=true` | `delete` | 同键置粘性墓碑；同优先级旧消息不能复活 |
| 版本回滚 | `kind=version` 且 `op=rollback` | `rollback` | 同版本身份键上落回滚状态，不新增行 |
| 真正新记录 | 身份指纹首次出现（新版本号/新评论/新国家评分） | `new` | 新行 |

### 稳定复合指纹（`identity.py`）
- `identity_fp`：只由身份元组派生（review=`app_id+review_id`，version=`app_id+version[+build]`，
  score=`app_id[+country]`，event=`id`）。与内容、来源、时间戳无关，**同身份永远同存储键**。
- `content_fp`：规范化业务内容（剔除来源/时间戳/op/传输字段，确定性 JSON + sha256），带规则版本前缀。
- 同版本不同 `build` 是不同身份；内容字段部分缺失按可得字段计算，身份字段缺失直接 quarantine。

### 来源优先级
`official < api < partner < web < rpa`（数字越小越权威，`identity.SOURCE_PRIORITY` 可调）。
低优先级来源的写入判 `suppressed`；同优先级按 `src` 字典序决胜，保证双源不来回翻转、与到达顺序无关。

### 乱序、缺失、重跑、迁移、升级
- 乱序批次：晚到的旧 `ts` 判 `stale`，绝不覆盖较新状态；最终状态与到达顺序无关。
- 部分字段缺失：身份缺陷（缺身份字段/坏时间戳/坏数值）→ 不可重试，进 `dedup_quarantine`，不产生 records 行。
- 重复运行：全部决策幂等；重放只增加审计行，不增加数据行。
- 历史迁移：`dedup.migrate_legacy()` 把 `records` 老行锚定进 `dedup_index`，幂等、不复制行；
  无法识别身份的老行进 quarantine。
- 规则升级：抬高 `identity.FP_RULE_VERSION` 后 `rebaseline_for_rule_version()` 只重建内容指纹基线，
  身份键与存储键不动，升级后首次重放判 `retransmit` 而非 `new`。
- 重启：状态全部在 sqlite，新进程 `bind()` 后从库恢复；`bind/flush/reset` 管理生命周期。

### 可重试 vs 不可重试
- 可重试：sqlite 瞬时故障包在 SAVEPOINT 内，回滚后抛 `DedupRetryable`，pipeline 有界重试（`MAX_RETRY`），
  失败不留半条状态。
- 不可重试：数据缺陷不抛异常，quarantine + 审计，记录不进入 `records` 表。

### 敏感数据
`identity.SENSITIVE_KEYS` 中的字段（token/secret/email/raw_text 等）只允许以
`<redacted:类型>` 或 sha256 摘要形式出现在诊断、quarantine、audit 中；测试含全表扫描断言。

## 跨模块改动与对应的数据不变量
| 模块 | 改动 | 保证的不变量 |
| --- | --- | --- |
| `identity.py`（新） | 身份元组、复合指纹、来源优先级、脱敏预览 | 同身份 ⇒ 同指纹，与内容/来源/时间无关 |
| `keyer.py` | typed 记录键 = identity_fp；无类型老记录保持原 `id` 键 | 编辑/删除/回滚永不换键；旧输出键值不漂移 |
| `store.py` | 新增 4 张 dedup 表与迁移/审计/隔离接口，`records` 表结构不变 | 状态可重启恢复；历史行与新状态共存不重复 |
| `dedup.py` | 朴素标记 → 状态机决策（8 种决策码 + 统计 + 诊断环） | 重传/编辑/回滚/新记录可区分；幽灵记录为零 |
| `pipeline.py` | 共享连接绑定、None 跳过落库、可重试有界重试、决策计数 | 被吸收记录物理上不可能落库；故障语义可观测 |
| `loader.py` | `load(path=None)` 支持测试夹具 | 默认夹具行为不变 |

## 持久化与测试
- `tests/golden/` 锁定改造前 `make run` 的指标与每行业务载荷（golden 测试逐字段比对）。
- `tests/test_dedup.py` 覆盖四分类、来源优先级、乱序、缺字段、重启、规则升级、历史迁移、
  可重试路径与敏感数据扫描；全部使用临时库，互不污染。
- 仍开放的功能缺口：`versionline.py`、`review_page.py` 的 `process()` 按基线保留 `NotImplementedError`。
