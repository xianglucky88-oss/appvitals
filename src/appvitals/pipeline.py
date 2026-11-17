"""薄编排：loader → normalizer→versionline→store_api→unify→score_align→dedup → keyer → store。不含业务判断。"""
import importlib
from . import store as _store
from . import loader as _loader
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'versionline', 'store_api', 'unify', 'score_align', 'dedup']

def run(con=None, records=None):
    own = con is None
    con = con or _store.connect()
    try:
        chain = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            chain.append(mod.process)
        fed = 0
        source = records if records is not None else _loader.load()
        for raw in source:
            rec = raw
            for fn in chain:
                rec = fn(rec)
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()
