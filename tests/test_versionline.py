"""versionline 记录语义与跨来源版本谱系回放测试。

场景覆盖（对应任务要求）：
- 成功：多商店完整输入走真实编排链入库，谱系可重建；
- 部分提交 / 重启：只重放缺失批次，不做全量重跑，谱系仅从已落库行恢复；
- 旧格式输入：别名字段与旧式版本/标签被规范化并保留溯源；
- 边界：缺失构建号、非法构建号、非法预发布标签、时区边界、回滚、同版多商店。
"""
import sqlite3

import pytest

from src.appvitals import pipeline, store, versionline


# --------------------------------------------------------------------------
# 夹具
# --------------------------------------------------------------------------

def _rec(rid, src, app, ver, released, build=None, pre=None):
    return {
        "id": rid,
        "src": src,
        "app": app,
        "version": ver,
        "build": build,
        "prerelease": pre,
        "released_at": released,
    }


def _lineage_records():
    """同一应用跨三商店、含回滚与同版再观察的发布序列。"""
    return [
        _rec("d-001", "alpha", "Demo", "1.0.0", "2026-11-01T00:00:00Z", build="100"),
        _rec("d-002", "alpha", "Demo", "2.0.0-alpha", "2026-11-03T00:00:00Z",
             build="199", pre="alpha"),
        _rec("d-003", "alpha", "Demo", "2.0.0", "2026-11-05T00:00:00Z", build="200"),
        # beta 商店晚到的同版本：同 version_key，不同 attribution_key
        _rec("d-004", "beta", "Demo", "2.0.0", "2026-11-06T00:00:00Z", build="200"),
        # 回滚：发布时间更晚，但版本低于已观察到的 2.0.0
        _rec("d-005", "alpha", "Demo", "1.5.0", "2026-11-08T00:00:00Z", build="150"),
        # 时区边界：与下一条是同一 UTC 瞬间（+09:00 与 +08:00 的本地日期不同）
        _rec("d-006", "gamma", "Demo", "1.6.0", "2026-11-10T01:00:00+09:00",
             build="160"),
        _rec("d-007", "beta", "Demo", "1.6.0", "2026-11-10T00:00:00+08:00",
             build="160"),
    ]


def _tmp_con(tmp_path):
    con = sqlite3.connect(str(tmp_path / "vl.db"))
    con.execute("CREATE TABLE IF NOT EXISTS records("
                "key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    return con


# --------------------------------------------------------------------------
# 单记录语义
# --------------------------------------------------------------------------

def test_process_canonicalizes_and_keeps_source_and_confidence():
    rec = versionline.process(_rec("d-001", "alpha", "Demo", "1.0.0",
                                   "2026-11-01T00:00:00Z", build="100"))
    vl = rec["version_line"]
    assert vl["canonical_version"] == "1.0.0"
    assert vl["version_core"] == [1, 0, 0]
    assert vl["build"] == 100 and vl["build_missing"] is False
    assert vl["released_epoch"] == 1793491200          # 2026-11-01T00:00:00Z
    assert vl["released_at_utc"] == "2026-11-01T00:00:00Z"
    assert vl["source"] == "alpha" and vl["source_id"] == "d-001"
    assert vl["confidence"] == 1.0 and vl["confidence_reasons"] == []
    assert len(vl["version_key"]) == 16 and len(vl["attribution_key"]) == 16
    # 原始字段不被破坏，溯源可回放
    assert rec["id"] == "d-001" and rec["released_at"] == "2026-11-01T00:00:00Z"
    assert rec["_stage_versionline"] == "ok"


def test_process_is_deterministic_and_idempotent():
    rec = _rec("d-003", "alpha", "Demo", "2.0.0", "2026-11-05T00:00:00Z",
               build="200")
    once = versionline.process(rec)
    twice = versionline.process(once)
    assert once["version_line"] == twice["version_line"]
    # 完全独立的两次处理得到相同幂等键
    again = versionline.process(dict(rec))
    assert again["version_line"]["version_key"] == once["version_line"]["version_key"]
    assert again["version_line"]["attribution_key"] == once["version_line"]["attribution_key"]


def test_prerelease_ordering_and_illegal_labels():
    base = {"id": "x", "src": "s", "app": "A", "released_at": "2026-11-01T00:00:00Z"}
    tags = ["1.0.0-alpha", "1.0.0-beta", "1.0.0-rc.1", "1.0.0"]
    keys = [versionline.process(dict(base, version=v, build="1"))["version_line"]["sort_key"]
            for v in tags]
    for lower, higher in zip(keys, keys[1:]):
        assert versionline._compare_keys(lower, higher) < 0

    # 非法标签：含空白 → 保留原值、标记非法、置信下降
    bad = versionline.process(dict(base, version="1.0.0", prerelease="be ta", build="1"))
    assert bad["version_line"]["prerelease_illegal"] is True
    assert "illegal_prerelease_label" in bad["version_line"]["confidence_reasons"]
    assert bad["version_line"]["confidence"] < 1.0
    # 未知标签名同样非法
    unknown = versionline.process(dict(base, version="1.0.0", prerelease="ga", build="1"))
    assert unknown["version_line"]["prerelease_illegal"] is True


def test_missing_and_illegal_build_are_explained():
    base = {"id": "x", "src": "s", "app": "A", "version": "1.0.0",
            "released_at": "2026-11-01T00:00:00Z"}
    missing = versionline.process(dict(base))
    assert missing["version_line"]["build"] is None
    assert missing["version_line"]["build_missing"] is True
    assert "missing_build" in missing["version_line"]["confidence_reasons"]

    illegal = versionline.process(dict(base, build="200a"))
    assert illegal["version_line"]["build"] is None
    assert illegal["version_line"]["build_illegal"] is True
    assert "illegal_build" in illegal["version_line"]["confidence_reasons"]


def test_timezone_boundary_normalizes_to_same_instant():
    a = versionline.process(_rec("t1", "gamma", "Demo", "1.6.0",
                                 "2026-11-10T01:00:00+09:00", build="160"))
    b = versionline.process(_rec("t2", "beta", "Demo", "1.6.0",
                                 "2026-11-10T00:00:00+08:00", build="160"))
    assert a["version_line"]["released_epoch"] == b["version_line"]["released_epoch"]
    assert a["version_line"]["released_at_utc"] == "2026-11-09T16:00:00Z"


def test_naive_timestamp_assumed_utc_and_flagged():
    out = versionline.process(_rec("n1", "alpha", "Demo", "1.0.0",
                                   "2026-11-01T00:00:00", build="100"))
    vl = out["version_line"]
    assert vl["released_at_naive"] is True
    assert "naive_timestamp_assumed_utc" in vl["confidence_reasons"]
    assert vl["released_epoch"] == 1793491200


def test_legacy_input_aliases_and_shapes():
    legacy = {
        "id": "l-001", "src": "gamma",
        "app_name": "Legacy", "ver": "v1.2", "version_code": "12",
        "release_tag": "RC", "published_at": "2026-10-01T00:00:00Z",
    }
    out = versionline.process(legacy)
    vl = out["version_line"]
    assert vl["legacy_format"] is True
    assert vl["canonical_version"] == "1.2.0-rc"      # 宽松版本补 PATCH，RC→rc
    assert vl["version_core"] == [1, 2, 0]
    assert vl["build"] == 12
    assert {"legacy_format", "legacy_version_shape", "legacy_tag_normalized"} \
        <= set(vl["confidence_reasons"])
    assert vl["released_epoch"] is not None
    # 旧字段原样保留，规范化结果出现在现行字段
    assert out["ver"] == "v1.2" and out["version"] == "1.2.0-rc"
    # 重复处理幂等键保持稳定（重启恢复的前提）
    again = versionline.process(dict(legacy))
    assert again["version_line"]["version_key"] == vl["version_key"]
    assert again["version_line"]["attribution_key"] == vl["attribution_key"]


def test_unparseable_version_does_not_raise_and_is_low_confidence():
    rec = {"id": "u1", "src": "s", "app": "A", "version": "not-a-version",
           "released_at": "2026-11-01T00:00:00Z"}
    out = versionline.process(rec)
    vl = out["version_line"]
    assert vl["core_valid"] is False
    assert "unparseable_version" in vl["confidence_reasons"]
    assert vl["confidence"] <= 0.5
    assert isinstance(vl["sort_key"], list)


def test_non_version_record_passes_through():
    out = versionline.process({"id": "plain", "src": "alpha", "name": "x", "value": 1})
    assert out["id"] == "plain" and "version_line" not in out
    assert out["_stage_versionline"] == "ok"


# --------------------------------------------------------------------------
# 跨来源谱系：排序、回滚、同版再观察
# --------------------------------------------------------------------------

def test_lineage_flags_rollback_reobservation_and_total_order():
    processed = [versionline.process(r) for r in _lineage_records()]
    lineage = versionline.build_lineage(processed)
    by_id = {r["id"]: r["version_line"] for r in lineage}

    # 回滚：1.5.0 晚于 2.0.0 发布；之后的 1.6.0 是回滚后的正常迭代，不再标记
    assert by_id["d-005"]["rollback"] is True
    assert all(by_id[i]["rollback"] is False
               for i in ["d-001", "d-002", "d-003", "d-004", "d-006", "d-007"])
    # 高水位不被回滚推低：1.6.0 < 2.0.0 但属于新发布而非再次回滚
    assert by_id["d-006"]["rollback"] is False

    # 同版多商店：d-004 是 d-003 的再观察，保留首次来源溯源
    assert by_id["d-004"]["reobserved"] is True
    assert by_id["d-004"]["first_seen_source"] == "alpha"
    assert by_id["d-004"]["first_seen_id"] == "d-003"
    assert by_id["d-003"]["reobserved"] is False
    assert by_id["d-003"]["version_key"] == by_id["d-004"]["version_key"]
    assert by_id["d-003"]["attribution_key"] != by_id["d-004"]["attribution_key"]

    # 时区同瞬间的两条构成再观察对，顺序由 (来源, 来源 id) 全确定性决胜
    tz_pair = {by_id["d-006"]["reobserved"], by_id["d-007"]["reobserved"]}
    assert tz_pair == {True, False}
    order = [r["id"] for r in lineage]
    assert order.index("d-007") < order.index("d-006")   # beta < gamma
    second = by_id["d-006"] if by_id["d-006"]["reobserved"] else by_id["d-007"]
    assert second["first_seen_id"] in {"d-006", "d-007"}
    # 全序列按发布时序排列
    epochs = [r["version_line"]["released_epoch"] for r in lineage]
    assert epochs == sorted(epochs)


def test_lineage_rebuild_is_pure_and_stable():
    processed = [versionline.process(r) for r in _lineage_records()]
    first = versionline.build_lineage(processed)
    second = versionline.build_lineage([dict(r) for r in processed])
    assert [r["id"] for r in first] == [r["id"] for r in second]
    assert all(a["version_line"]["version_key"] == b["version_line"]["version_key"]
               for a, b in zip(first, second))


# --------------------------------------------------------------------------
# 编排回放：成功 / 部分提交 / 重启 / 旧格式（不做全量重跑）
# --------------------------------------------------------------------------

def test_pipeline_success_replay_via_real_chain(tmp_path):
    con = _tmp_con(tmp_path)
    try:
        result = pipeline.run(con=con, records=_lineage_records())
        assert result["fed"] == 7 and result["rows"] == 7
        # 再跑一次：相同键的 upsert 不增行
        again = pipeline.run(con=con, records=_lineage_records())
        assert again["rows"] == 7

        persisted = [d for _, d in store.all_records(con)]
        assert all("version_line" in d for d in persisted)
        lineage = versionline.build_lineage(persisted)
        by_id = {r["id"]: r["version_line"] for r in lineage}
        assert by_id["d-005"]["rollback"] is True
        assert by_id["d-004"]["reobserved"] is True
    finally:
        con.close()


def test_partial_commit_then_restart_replays_only_missing_batch(tmp_path):
    db = tmp_path / "vl.db"
    full = _lineage_records()
    first_batch, second_batch = full[:4], full[4:]

    # 第一批已提交（部分提交），随后进程中断
    con1 = sqlite3.connect(str(db))
    con1.execute("CREATE TABLE records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    assert pipeline.run(con=con1, records=first_batch)["rows"] == 4
    con1.close()

    # 未提交的写入在重启后必须丢失，且恢复不依赖进程内状态
    ghost = sqlite3.connect(str(db))
    ghost.execute("CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    from src.appvitals.models import Record
    from src.appvitals.keyer import key_of
    store.upsert(ghost, Record(key=key_of(versionline.process(second_batch[0])),
                               data=versionline.process(second_batch[0])))
    ghost.close()  # 未 commit

    # 重启：新连接，只重放缺失的第二批（不做全量重跑）
    con2 = sqlite3.connect(str(db))
    con2.execute("CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    try:
        assert store.count(con2) == 4                 # 未提交行未污染存储
        result = pipeline.run(con=con2, records=second_batch)
        assert result["fed"] == 3 and result["rows"] == 7

        # 谱系仅从已落库行重建：回滚/再观察/幂等键全部稳定
        lineage = versionline.build_lineage([d for _, d in store.all_records(con2)])
        by_id = {r["id"]: r["version_line"] for r in lineage}
        assert by_id["d-005"]["rollback"] is True
        assert by_id["d-004"]["reobserved"] is True
        expected = {r["id"]: versionline.process(r)["version_line"]["version_key"]
                    for r in full}
        assert all(by_id[i]["version_key"] == k for i, k in expected.items())

        # 再重启一次，不重放任何数据，结果依旧稳定
        con3 = sqlite3.connect(str(db))
        try:
            reborn = versionline.build_lineage([d for _, d in store.all_records(con3)])
            assert [r["id"] for r in reborn] == [r["id"] for r in lineage]
        finally:
            con3.close()
    finally:
        con2.close()


def test_legacy_records_flow_through_pipeline_with_stable_ids(tmp_path):
    legacy = [
        {"id": "lg-1", "src": "gamma", "app_name": "Legacy", "ver": "v1.0",
         "version_code": "10", "release_tag": "alpha",
         "published_at": "2026-10-01T00:00:00Z"},
        {"id": "lg-2", "src": "gamma", "app_name": "Legacy", "ver": "1.0.1",
         "version_code": "11", "release_tag": "",
         "published_at": "2026-10-02T00:00:00Z"},
    ]
    con = _tmp_con(tmp_path)
    try:
        assert pipeline.run(con=con, records=legacy)["rows"] == 2
        rows = {k: d for k, d in store.all_records(con)}
        # 现有 keyer 语义不变：以原始 id 为稳定主键
        assert set(rows) == {"lg-1", "lg-2"}
        lineage = versionline.build_lineage(list(rows.values()))
        assert [r["id"] for r in lineage] == ["lg-1", "lg-2"]
        assert all(r["version_line"]["legacy_format"] for r in lineage)
        # 重放不增行
        assert pipeline.run(con=con, records=legacy)["rows"] == 2
    finally:
        con.close()
