"""review_page 接入 pipeline 的最小集成契约：
摊平一页多条、keyer/store 幂等落库、老格式记录透传不受影响、下游阶段标记齐全。"""
from appvitals import (
    pipeline, store, review_page, normalizer, store_api,
    unify, score_align, dedup,
)
from appvitals.keyer import key_of
from appvitals.models import Record

from conftest import load_fixture

CHAIN = [normalizer.process, review_page.process, store_api.process,
         unify.process, score_align.process, dedup.process]


def _ingest(con, rows):
    review_page.reset_state()
    fed = 0
    for raw in rows:
        batch = [raw]
        for fn in CHAIN:
            batch = [out for rec in batch for out in pipeline._as_list(fn(rec))]
        for rec in batch:
            store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
    store.commit(con)
    return fed


def test_pipeline_flattens_review_pages_and_persists(tmp_path, monkeypatch):
    db = tmp_path / "integ.db"
    monkeypatch.setattr(store, "DB_PATH", db)
    envelopes = load_fixture("first_import")
    legacy = [{"id": "r-001", "src": "alpha", "name": " Legacy ", "value": 10,
               "status": "active", "ts": "2026-11-01T00:00:00Z"}]

    con = store.connect()
    fed1 = _ingest(con, envelopes + legacy)
    rows1 = dict(store.all_records(con))

    # 4 个 envelope：4 条评论 + 每 envelope 一份断点（同 id 幂等覆盖为 2 行）+ 1 条透传
    assert fed1 == 9                                  # upsert 调用次数（含断点覆盖）
    assert store.count(con) == 7                      # 实际落库唯一行数
    kinds = {}
    for d in rows1.values():
        kinds[d.get("kind")] = kinds.get(d.get("kind"), 0) + 1
    assert kinds == {"review": 4, "review_page_checkpoint": 2, None: 1}

    # 评论记录穿过全部下游阶段，阶段标记齐全；老记录不被 review_page 触碰
    review = next(d for d in rows1.values() if d.get("kind") == "review")
    for tag in ("_stage_review_page", "_stage_store_api", "_stage_unify",
                "_stage_score_align", "_stage_dedup"):
        assert tag in review
    assert "_stage_review_page" not in rows1["r-001"]
    assert rows1["r-001"]["name"] == "Legacy"     # normalizer 仍先于 review_page 生效

    # 全量重跑：幂等 upsert，行数不增、键集合不变
    fed2 = _ingest(con, envelopes + legacy)
    assert fed2 == fed1
    assert store.count(con) == 7
    assert dict(store.all_records(con)).keys() == rows1.keys()
    con.close()


def test_pipeline_run_with_default_fixture_unchanged(monkeypatch, tmp_path):
    # 基线 fixtures/input.json 全部走透传分支，pipeline.run 对外行为不变
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "base.db")
    review_page.reset_state()
    assert pipeline.run() == {"fed": 4, "rows": 3}
    review_page.reset_state()
