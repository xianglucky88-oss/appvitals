"""review_page：跨商店评论分页可恢复采集的行为契约测试。

覆盖：首次导入（页码+游标、空页）、重复批次（页内/跨页重复 ID、编辑/删除、
循环停止、重放幂等）、缺字段（缺评分/缺 id/revision 非法/缺 next_cursor/非法信封）、
乱序输入（六种页序收敛一致、缺口跟踪）、中途恢复（页级阻断 + sqlite 断点重建）。
"""
import itertools

from appvitals import review_page as rp
from appvitals.keyer import key_of

from conftest import load_fixture, replay, index_by_id  # noqa: F401


# ---------------------------------------------------------------- 首次导入
def test_first_import_page_and_cursor_modes(clean_state):
    envelopes = load_fixture("first_import")
    records, receipts = replay(clean_state, envelopes)

    # 停止条件：两个商店都以 end_of_pages 正常收敛
    assert receipts[1]["status"] == "stopped"
    assert receipts[1]["stop_reason"] == "end_of_pages"
    assert receipts[3]["status"] == "stopped"
    assert receipts[3]["stop_reason"] == "end_of_pages"

    by_id = index_by_id(records)
    # 输出键契约：评论实体、断点各自稳定
    assert set(by_id) == {
        "rv:app-demo:alpha:rv-1", "rv:app-demo:alpha:rv-2",
        "rv:app-demo:alpha:rv-3", "rpckpt:app-demo:alpha",
        "rv:app-demo:beta:rv-9", "rpckpt:app-demo:beta",
    }
    for rec in records:
        assert rec["_stage_review_page"] == "ok"
        assert key_of(rec) == rec["id"]          # keyer 可直接派生稳定键

    alpha = by_id["rpckpt:app-demo:alpha"]
    beta = by_id["rpckpt:app-demo:beta"]
    assert alpha["mode"] == "page_number"
    assert alpha["consumed"] == ["n:1", "n:2"]
    assert alpha["exhausted"] is True
    assert beta["mode"] == "cursor"
    assert beta["consumed"] == ["c:c2", "c:start"]
    assert beta["exhausted"] is True

    rv1 = by_id["rv:app-demo:alpha:rv-1"]
    assert rv1["kind"] == "review"
    assert rv1["status"] == "active"
    assert rv1["version"] == "1.0.0"            # 版本关联
    assert rv1["history"][0]["event"] == "upsert"


def test_empty_page_is_not_an_error(clean_state):
    records, receipts = replay(clean_state, load_fixture("first_import"))
    beta_receipt = receipts[3]
    assert beta_receipt["status"] == "stopped"  # 空页 + has_more=false 正常结束
    assert beta_receipt["applied"] == 0
    assert all(r["kind"] != "review_page_error" for r in beta_receipt["records"])
    sess = clean_state._STATE["app-demo\x1fbeta"]
    assert sess.stats["empty_pages"] == 1
    assert sess.stats["blocked_pages"] == 0


# ---------------------------------------------------------------- 重复批次
def test_duplicate_batch_idempotent_and_edit_delete_converge(clean_state):
    envelopes = load_fixture("duplicate_batch")

    first, _ = replay(clean_state, envelopes)
    clean_state.reset_state()
    second, _ = replay(clean_state, envelopes)

    # 重复运行产出的键集合完全一致（upsert 幂等，落库不增行）
    assert sorted(r["id"] for r in first) == sorted(r["id"] for r in second)

    sess = clean_state._STATE["app-dup\x1falpha"]
    rv1, rv2 = sess.reviews["rv-1"], sess.reviews["rv-2"]
    # 编辑：新版本关联、字段更新、旧评分被显式覆盖
    assert rv1["status"] == "active"
    assert rv1["revision"] == 2
    assert rv1["version"] == "1.1.0"
    assert rv1["rating"] == 4
    assert rv1["title"] == "good (edited)"
    assert [h["event"] for h in rv1["history"]] == ["upsert", "edit"]
    # 删除：状态翻转，时间线保留
    assert rv2["status"] == "deleted"
    assert rv2["revision"] == 2
    assert [h["event"] for h in rv2["history"]] == ["upsert", "delete"]

    # 页内重复 review id 在 winner 阶段只保留一条，不产生重复评论实体
    assert sess.stats["events_seen"] == 4
    assert sess.stats["events_applied"] == 4     # rv-1 upsert, rv-2 upsert, edit, delete
    assert len([r for r in first if r["kind"] == "review"]) == 4


def test_pagination_loop_stops_without_touching_timeline(clean_state):
    envelopes = load_fixture("duplicate_batch")
    _, receipts = replay(clean_state, envelopes)
    loop = receipts[2]                            # 重发 page 1
    assert loop["status"] == "stopped"
    assert loop["stop_reason"] == "pagination_loop"
    assert all(r["kind"] != "review" for r in loop["records"])  # 无评论被改写
    sess = clean_state._STATE["app-dup\x1falpha"]
    assert sess.cur.loop is True
    assert sess.stats["loop_pages"] == 1


def test_stale_event_cannot_roll_back_newer_state(clean_state):
    base = {"kind": "review_page", "app_id": "a", "store": "s",
            "page_number": 1, "has_more": False, "reviews": []}
    clean_state.process_page({**base, "reviews": [
        {"id": "x", "event": "upsert", "revision": 3, "ts": "t3",
         "rating": 5, "version": "3.0"}]})
    # 旧 revision 从后续新页迟到：必须被忽略，不能回滚收敛态
    clean_state.process_page({**base, "page_number": 2, "reviews": [
        {"id": "x", "event": "upsert", "revision": 1, "ts": "t1",
         "rating": 1, "version": "1.0"}]})
    sess = clean_state._STATE["a\x1fs"]
    assert sess.reviews["x"]["revision"] == 3
    assert sess.reviews["x"]["rating"] == 5
    assert sess.stats["events_duplicate"] == 1


# ---------------------------------------------------------------- 缺字段
def test_missing_fields_are_classified_not_fatal(clean_state):
    envelopes = load_fixture("missing_fields")
    records, receipts = replay(clean_state, envelopes)

    # 第一页整体成功，坏事件被逐条分类
    page1 = receipts[0]
    assert page1["status"] == "stopped"
    sess = clean_state._STATE["app-miss\x1falpha"]
    assert sess.stats["events_invalid"] == 3       # 缺 id / revision 非法 / 非对象
    assert sess.stats["missing_rating"] == 2       # 无 rating 键 + rating=null
    valid = {r["review_id"] for r in records
             if r.get("app_id") == "app-miss" and r.get("store") == "alpha"
             and r["kind"] == "review"}
    assert valid == {"rv-1", "rv-4"}
    rv1 = next(r for r in records if r.get("id") == "rv:app-miss:alpha:rv-1")
    assert rv1["rating_missing"] is True
    assert rv1["rating"] is None

    errors = [r for r in records if r["kind"] == "review_page_error"]
    codes = {r["code"] for r in errors}
    assert "invalid_event" in codes
    # 错误记录有稳定 id：干净状态重放同一批次，错误 id 集合一致（落库不增新行）
    clean_state.reset_state()
    again, _ = replay(clean_state, envelopes)
    err_ids_a = sorted(r["id"] for r in records if r["kind"] == "review_page_error")
    err_ids_b = sorted(r["id"] for r in again if r["kind"] == "review_page_error")
    assert err_ids_a == err_ids_b


def test_missing_next_cursor_blocks_and_checkpoint_holds(clean_state):
    envelopes = load_fixture("missing_fields")
    _, receipts = replay(clean_state, envelopes)
    blocked = receipts[1]
    assert blocked["status"] == "blocked"
    assert blocked["stop_reason"] == "missing_next_cursor"
    sess = clean_state._STATE["app-miss\x1fbeta"]
    assert sess.blocked is True
    assert sess.cur is None or sess.cur.consumed == set()   # 断点不前进

    # 修正批次（带 next_cursor）可在同位置解除阻断继续
    fixed = {"kind": "review_page", "app_id": "app-miss", "store": "beta",
             "cursor": "start", "next_cursor": "c2", "has_more": False,
             "reviews": [{"id": "rv-9", "event": "upsert", "revision": 1,
                          "ts": "t", "rating": 2}]}
    receipt = clean_state.process_page(fixed)
    assert receipt["status"] == "stopped"
    assert clean_state._STATE["app-miss\x1fbeta"].blocked is False


def test_invalid_envelope_never_creates_session(clean_state):
    receipts = []
    for env in load_fixture("missing_fields"):
        receipts.append(clean_state.process_page(env))
    last = receipts[-1]
    assert last["status"] == "blocked"
    assert last["stop_reason"] == "invalid_envelope"
    assert all(r["kind"] != "review_page_checkpoint" for r in last["records"])
    assert "app-miss\x1f" not in clean_state._STATE  # 没有 store 名无法建会话


# ---------------------------------------------------------------- 乱序
def test_out_of_order_pages_converge_under_every_permutation(clean_state):
    envelopes = load_fixture("out_of_order")
    idx = {e["page_number"]: e for e in envelopes}

    def snapshot(order):
        clean_state.reset_state()
        for n in order:
            clean_state.process_page(idx[n])
        cur = clean_state._STATE["app-ooo\x1falpha"].cur
        reviews = clean_state._STATE["app-ooo\x1falpha"].reviews
        return (
            tuple(sorted((rid, d["status"], d["revision"], d["version"])
                         for rid, d in reviews.items())),
            tuple(sorted(cur.consumed)),
            tuple(sorted(cur.gap_positions)),
            cur.exhausted,
        )

    outcomes = {snapshot(list(p)) for p in itertools.permutations([1, 2, 3])}
    assert len(outcomes) == 1
    reviews, consumed, gaps, exhausted = outcomes.pop()
    assert exhausted is True
    assert gaps == ()
    assert consumed == ("n:1", "n:2", "n:3")
    assert dict((rid, (status, ver)) for rid, status, _rev, ver in reviews) == {
        "rv-1": ("active", "1.0.0"),
        "rv-2": ("active", "1.1.0"),
        "rv-3": ("active", "1.2.0"),
    }


def test_gap_positions_tracked_and_cleared(clean_state):
    base = {"kind": "review_page", "app_id": "g", "store": "s"}
    clean_state.process_page({**base, "page_number": 1, "has_more": True, "reviews": []})
    r3 = clean_state.process_page({**base, "page_number": 3, "has_more": False, "reviews": []})
    assert r3["out_of_order"] is True
    sess = clean_state._STATE["g\x1fs"]
    assert sess.cur.gap_positions == {"n:2"}
    assert sess.cur.exhausted is False                 # 有缺口不允许耗尽
    clean_state.process_page({**base, "page_number": 2, "has_more": False, "reviews": []})
    assert sess.cur.gap_positions == set()
    assert sess.cur.exhausted is True                  # 缺口补齐后收敛


# ---------------------------------------------------------------- 中途恢复
def test_recovery_fixture_block_and_resume_in_process(clean_state):
    envelopes = load_fixture("recovery")
    _, receipts = replay(clean_state, envelopes)
    reasons = [r["stop_reason"] for r in receipts]
    # 页码链：非法 payload 阻断 -> 同位置修正页恢复；游标链：坏锚点阻断 -> 修正锚点恢复
    assert reasons == [
        None, "invalid_payload", "end_of_pages",
        None, "anchor_invalid", "end_of_pages",
    ]
    alpha = clean_state._STATE["app-rec\x1falpha"]
    beta = clean_state._STATE["app-rec\x1fbeta"]
    assert alpha.blocked is False and alpha.cur.exhausted is True
    assert beta.blocked is False and beta.cur.exhausted is True
    assert set(alpha.reviews) == {"rv-1", "rv-2"}
    assert set(beta.reviews) == {"rv-9", "rv-10"}
    # 阻断页不前进断点：alpha 只消费 1、2 两页，没有为坏 payload 留位置
    assert alpha.cur.consumed == {"n:1", "n:2"}


def test_cross_process_recovery_from_sqlite(tmp_path, monkeypatch):
    from appvitals import store
    from appvitals.models import Record

    db = tmp_path / "rec.db"
    monkeypatch.setattr(store, "DB_PATH", db)
    envelopes = load_fixture("recovery")

    # 第一次运行：跑到 page2 非法 payload 后中断（模拟进程崩溃）
    con = store.connect()
    rp.reset_state()
    for env in envelopes[:2]:
        for rec in rp.process(env):
            store.upsert(con, Record(key=key_of(rec), data=rec))
    store.commit(con)
    con.close()
    rows_after_crash = None

    # 新进程：仅靠 sqlite 里的断点/评论/错误重建内存状态
    con = store.connect()
    state = rp.load_state(con)
    sess = state["app-rec\x1falpha"]
    assert sess.blocked is True
    assert sess.block_reason == "invalid_payload"
    assert sess.cur.consumed == {"n:1"}
    assert set(sess.reviews) == {"rv-1"}
    assert ("rv-1", "upsert", 1, "2026-11-01T08:00:00Z") in sess.applied

    before = store.count(con)
    for env in envelopes[2:]:
        for rec in rp.process(env, state=state):
            store.upsert(con, Record(key=key_of(rec), data=rec))
    store.commit(con)

    alpha, beta = state["app-rec\x1falpha"], state["app-rec\x1fbeta"]
    assert not alpha.blocked and alpha.cur.exhausted
    assert not beta.blocked and beta.cur.exhausted
    # 已应用事件重放为去重命中，不产生重复评论行
    review_rows = [d for _k, d in store.all_records(con)
                   if d.get("kind") == "review"]
    assert len(review_rows) == 4
    assert store.count(con) > before
    con.close()


# ---------------------------------------------------------------- 入口契约
def test_process_passthrough_keeps_unrelated_records_byte_stable(clean_state):
    legacy = {"id": "r-001", "src": "alpha", "name": "x", "value": 1}
    out = clean_state.process(dict(legacy))
    assert out == legacy                               # 不加字段、不改键
    assert "_stage_review_page" not in out


def test_process_returns_flat_record_list_for_envelope(clean_state):
    out = clean_state.process(load_fixture("first_import")[0])
    assert isinstance(out, list)
    assert all(isinstance(r, dict) for r in out)
    assert {r["kind"] for r in out} == {"review", "review_page_checkpoint"}


# ---------------------------------------------------------------- 锚点/增量开窗
def test_anchor_required_on_first_cursor_page_when_provided(clean_state):
    bad = {"kind": "review_page", "app_id": "a", "store": "s",
           "cursor": "start", "anchor": "BOGUS", "has_more": False, "reviews": []}
    r = clean_state.process_page(bad)
    assert r["status"] == "blocked"
    assert r["stop_reason"] == "anchor_invalid"
    # 去掉锚点（上游首页约定 null）即正常
    ok = dict(bad)
    ok.pop("anchor")
    assert clean_state.process_page(ok)["status"] == "stopped"


def test_incremental_reopen_after_exhaustion(clean_state):
    base = {"kind": "review_page", "app_id": "w", "store": "s"}
    clean_state.process_page({**base, "page_number": 1, "has_more": False, "reviews": []})
    sess = clean_state._STATE["w\x1fs"]
    assert sess.cur.exhausted is True

    # 严格接续的新窗口：允许重新开窗
    r2 = clean_state.process_page(
        {**base, "page_number": 2, "has_more": False,
         "reviews": [{"id": "n1", "event": "upsert", "revision": 1,
                      "ts": "t", "rating": 5}]})
    assert r2["status"] == "stopped"
    assert sess.cur.exhausted is True
    assert "n1" in sess.reviews

    # 跳位开窗（不接续）拒绝；已消费旧页判循环
    r_gap = clean_state.process_page({**base, "page_number": 4, "has_more": False,
                                      "reviews": []})
    assert r_gap["stop_reason"] == "after_exhaustion"
    r_loop = clean_state.process_page({**base, "page_number": 1, "has_more": False,
                                       "reviews": []})
    assert r_loop["stop_reason"] == "pagination_loop"


def test_loaded_state_last_key_tracks_final_history_event(tmp_path, monkeypatch):
    # 回归：删除后跨进程恢复，_last_key 必须取自 history 末条（delete），
    # 否则同 revision 的迟到事件可能让已删除评论复活。
    from appvitals import store
    from appvitals.models import Record

    monkeypatch.setattr(store, "DB_PATH", tmp_path / "lk.db")
    con = store.connect()
    rp.reset_state()
    pages = [
        {"kind": "review_page", "app_id": "lk", "store": "s", "page_number": 1,
         "has_more": True,
         "reviews": [{"id": "d1", "event": "upsert", "revision": 1, "ts": "t1",
                      "rating": 5, "version": "1.0"}]},
        {"kind": "review_page", "app_id": "lk", "store": "s", "page_number": 2,
         "has_more": False,
         "reviews": [{"id": "d1", "event": "delete", "revision": 2, "ts": "t2"}]},
    ]
    for env in pages:
        for rec in rp.process(env):
            store.upsert(con, Record(key=key_of(rec), data=rec))
    store.commit(con)

    state = rp.load_state(con)
    sess = state["lk\x1fs"]
    assert sess.reviews["d1"]["status"] == "deleted"
    # 同 revision=2 的迟到 upsert（总序低于 delete）不得复活
    stale = {"kind": "review_page", "app_id": "lk", "store": "s",
             "page_number": 3, "has_more": False,
             "reviews": [{"id": "d1", "event": "upsert", "revision": 2, "ts": "t2",
                          "rating": 1}]}
    rp.process_page(stale, state=state)
    assert state["lk\x1fs"].reviews["d1"]["status"] == "deleted"
    con.close()
