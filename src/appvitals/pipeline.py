"""薄编排：loader → normalizer→review_page→store_api→unify→score_align→dedup → keyer → store。
不含业务判断。阶段可返回单条记录（透传）或记录列表（如 review_page 一页多条），
编排层只负责摊平，不解释业务字段。"""
import importlib
from collections.abc import Mapping
from . import store as _store
from . import loader as _loader
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'review_page', 'store_api', 'unify', 'score_align', 'dedup']

def run(con=None):
    own = con is None
    con = con or _store.connect()
    try:
        chain = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            chain.append(mod.process)
        fed = 0
        for raw in _loader.load():
            batch = [raw]
            for fn in chain:
                batch = [out for rec in batch
                         for out in _as_list(fn(rec))]
            for rec in batch:
                _store.upsert(con, Record(key=key_of(rec), data=rec))
                fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()

def _as_list(out):
    if isinstance(out, Mapping) or not isinstance(out, (list, tuple)):
        return [out]
    return list(out)
