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
- `review_page.py`：`process()` 当前抛出 `NotImplementedError`，未接入基础 happy-path。
- `score_align.py`：提供基础 `process()`，复制输入记录并补充 `_stage_score_align` 阶段标记。
- `store_api.py`：提供基础 `process()`，复制输入记录并补充 `_stage_store_api` 阶段标记。
- `unify.py`：提供基础 `process()`，复制输入记录并补充 `_stage_unify` 阶段标记。
- `dedup.py`：提供基础 `process()`，复制输入记录并补充 `_stage_dedup` 阶段标记。

当前未实现阶段未进入基础流水线，因此 `make run` 仍可运行已接入的阶段。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
