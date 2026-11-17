# appvitals — 跨商店监控应用版本更新、评分与评论分页，关联版本发布与口碑变化。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalizer` → 其余已实现变换 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线 happy-path，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线、幂等和边界桩
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`versionline`, `review_page`, `score_align`, `store_api`, `unify`, `dedup`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli`。

## 当前实现状态
- `versionline.py`：`process()` 当前抛出 `NotImplementedError`，未接入基础 happy-path。
- `review_page.py`：已实现并接入流水线（位于 normalizer 之后）。非评论页记录原样透传，
  评论页 envelope 返回记录列表由编排层摊平；happy-path 基线输入全部走透传分支，对外行为不变。
- `score_align.py`：提供基础 `process()`，复制输入记录并补充 `_stage_score_align` 阶段标记。
- `store_api.py`：提供基础 `process()`，复制输入记录并补充 `_stage_store_api` 阶段标记。
- `unify.py`：提供基础 `process()`，复制输入记录并补充 `_stage_unify` 阶段标记。
- `dedup.py`：提供基础 `process()`，复制输入记录并补充 `_stage_dedup` 阶段标记。

### review_page 分页采集契约
输入为 `kind="review_page"` 的 envelope，同时支持页码（`page_number`）与游标
（`cursor/next_cursor/anchor`）两种分页，评论事件支持 `upsert/edit/delete` 与 `revision`。
输出三类稳定键记录，均经 keyer/store 幂等落库：

| 记录 kind | 稳定 id | 含义 |
| --- | --- | --- |
| `review` | `rv:{app_id}:{store}:{review_id}` | 评论收敛态（含 history、版本关联、评分缺失标记） |
| `review_page_checkpoint` | `rpckpt:{app_id}:{store}` | 断点：已消费页、缺口、末页声明、阻断态、统计 |
| `review_page_error` | `rperr:{app}:{store}:{code}:{hash}` | 错误分类，内容哈希保证重放不增行 |

停止条件：`end_of_pages`（空页/末页且无缺口）、`pagination_loop`（重复页）、
`after_exhaustion`（耗尽后非接续页）、`blocked`（锚点失效、缺游标/页码、模式切换、
payload 非法、缺 next_cursor）。页级异常不前进断点，修正批次可从断点重放；
单条事件异常只计入错误分类，不拖垮整页。乱序页按总序键 `(revision, ts, event)` 收敛，
与到达顺序无关；跨进程恢复用 `review_page.load_state(con)` 从 sqlite 重建会话。
离线夹具位于 `fixtures/review_pages/`，回放断言见 `tests/test_review_page.py`。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
