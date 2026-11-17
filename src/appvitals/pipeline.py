"""薄编排：loader → normalizer→store_api→unify→score_align→dedup → keyer → store。

编排层不含业务判断，只负责：
- 共用一个 sqlite 连接绑定给 dedup 状态机（重启后状态从库中恢复）；
- dedup 返回 None 的记录（重传/抑制/过期/隔离）跳过落库，杜绝幽灵行；
- DedupRetryable 走有界重试（可重试路径）；其余数据缺陷已在 dedup 内 quarantine；
- 结束时 flush 并汇总各决策计数，供调用方/测试核对最终状态。
"""
import importlib
from . import store as _store
from . import loader as _loader
from . import dedup as _dedup
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'store_api', 'unify', 'score_align', 'dedup']
MAX_RETRY = 2  # 每条记录的可重试尝试上限（首次 + 2 次重试）


def run(con=None):
    own = con is None
    con = con or _store.connect()
    try:
        _dedup.bind(con)  # 状态与流水线同连接、同事务边界
        stages = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            stages.append((n, mod.process))

        fed = stored = 0
        for raw in _loader.load():
            rec = raw
            for name, fn in stages:
                if name == "dedup":
                    rec = _run_with_retry(fn, rec)
                else:
                    rec = fn(rec)
            fed += 1
            if rec is None:
                continue  # dedup 吸收：重传/低优先级/过期/隔离，不产生任何 records 行
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            stored += 1
        con.commit()
        dstats = _dedup.flush()
        return {
            "fed": fed,
            "stored": stored,
            "rows": _store.count(con),
            "retried": dstats.get("retry", 0),
            "dedup": dstats,
        }
    finally:
        if own:
            con.close()


def _run_with_retry(fn, rec):
    """可重试路径：仅对 DedupRetryable 做有界重试；其它异常直接上抛（不可重试语义已在内部隔离）。"""
    attempt = 0
    while True:
        try:
            return fn(rec)
        except _dedup.DedupRetryable:
            attempt += 1
            if attempt > MAX_RETRY:
                raise
