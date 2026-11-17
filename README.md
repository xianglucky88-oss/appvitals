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
- `score_align.py`：已实现评分对齐；保持 `process(record) -> dict` 接口与既有字段不变，派生
  诊断写入 `_score_align`（schema `score_align.v1`）。覆盖统一时间轴/时区换算、跨店铺评分
  口径归一、半星精度、评论数量核对与去重、迟到评论、无评论区间推导与失败恢复。
- `store_api.py`：提供基础 `process()`，复制输入记录并补充 `_stage_store_api` 阶段标记。
- `unify.py`：提供基础 `process()`，复制输入记录并补充 `_stage_unify` 阶段标记。
- `dedup.py`：提供基础 `process()`，复制输入记录并补充 `_stage_dedup` 阶段标记。

当前未实现阶段未进入基础流水线，因此 `make run` 仍可运行已接入的阶段。

## score_align 数据不变量
1. 所有时间点先归一为带时区时刻再折算 UTC，同时保留原始本地偏移；裸时间按声明时区解释。
2. 跨店铺评分按 `rating_scale` / `rating_unit` 归一到 0..5 星，`percent`、分制与 `"a/b"` 均可解析。
3. 评分量化到最近 0.5 星（`ROUND_HALF_UP`），以 0..10 的 `half_steps` 表达，原始值保留。
4. `review_count.reported`（声明）与 `observed`（去重后可观测）分离，重复导入不放大数量/分布。
5. `no_review_windows` 只由显式零评论窗口或"评论事件完整"的覆盖区间推导，稀疏评论不臆造零值。
6. 迟到评论（显式标记 / 超阈值滞后 / 晚于观测）计入 `late`，但仍参与数量、分布与时间轴。
7. `process` 无模块级可变状态，`process(process(r)) == process(r)`，并发调用结果一致。
8. 畸形字段降级为 `warnings` 诊断，单条坏评论不拖累其他评论；非映射入参抛 `TypeError`。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
