"""score_align 回归：数据不变量、接口兼容、诊断输出。

覆盖题目要求的四类回归：旧输入兼容、重复导入、并发/重入、失败恢复，
外加时间轴、评分口径、无评论区间、迟到评论、半星精度与时区换算。
"""
import copy
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.appvitals import score_align
from src.appvitals.score_align import process


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
def diag(record):
    return process(record)["_score_align"]


def basic(record=None, **extra):
    payload = {
        "id": "s-001",
        "store": "alpha",
        "version": "1.4.0",
        "rating": 4.4,
        "review_count": 100,
        "released_at": "2026-10-01T00:00:00Z",
        "observed_at": "2026-11-01T00:00:00Z",
    }
    payload.update(extra)
    if record:
        payload.update(record)
    return payload


def parsing(*entries, **extra):
    return basic(reviews=list(entries), **extra)


# --------------------------------------------------------------------------- #
# 接口兼容：旧输入与字段保留
# --------------------------------------------------------------------------- #
def test_legacy_record_is_passthrough_and_fields_untouched():
    original = {
        "id": "r-001", "src": "alpha", "name": " Record One ", "value": 10,
        "status": "active", "ts": "2026-11-01T00:00:00Z",
    }
    snapshot = copy.deepcopy(original)
    out = process(original)

    # 原有字段逐字段不变，输入对象本身不被修改。
    for key, value in snapshot.items():
        assert out[key] == value
    assert original == snapshot
    # 唯一新增的顶层字段是诊断块；基线阶段标记保持原值。
    assert set(out) - set(original) == {"_score_align", "_stage_score_align"}
    assert out["_stage_score_align"] == "naive"
    assert out["_score_align"]["status"] == "passthrough"


def test_record_with_only_unparseable_fields_is_recovered_not_passthrough():
    block = diag({"id": "x", "rating": "N/A", "released_at": "soon"})
    assert block["status"] == "recovered"
    assert {w["code"] for w in block["warnings"]} >= {"invalid_rating", "invalid_timestamp"}


def test_pipeline_records_still_flow_through_unchanged():
    from src.appvitals import pipeline, store
    before = pipeline.run()
    after = pipeline.run()
    assert before["fed"] == 4
    assert after["rows"] == before["rows"]
    con = store.connect()
    try:
        for key, data in store.all_records(con):
            assert data["_stage_score_align"] == "naive"
            assert data["_score_align"]["schema"] == score_align.SCHEMA
    finally:
        con.close()


def test_existing_stage_marker_is_not_overwritten():
    out = process({"id": "x", "_stage_score_align": "custom"})
    assert out["_stage_score_align"] == "custom"


def test_non_mapping_input_is_interface_error():
    with pytest.raises(TypeError):
        process(["not", "a", "mapping"])
    with pytest.raises(TypeError):
        process("string")


def test_diagnostics_schema_is_stable():
    block = diag(basic())
    assert block["schema"] == "score_align.v1"
    assert set(block) == {
        "schema", "status", "store", "version", "observation_at_utc", "rating",
        "review_count", "star_distribution", "timeline", "no_review_windows",
        "gaps_computable", "warnings",
    }
    assert set(block["review_count"]) == {
        "reported", "observed", "duplicates", "late", "late_ids", "basis",
    }
    assert set(block["timeline"]) == {
        "release_at_utc", "observation_at_utc", "first_review_at_utc",
        "last_review_at_utc", "release_to_observation_seconds", "timezone",
        "timezone_source", "assumed_timezone", "events", "events_truncated",
    }


# --------------------------------------------------------------------------- #
# 时间轴
# --------------------------------------------------------------------------- #
def test_timeline_puts_release_observation_reviews_on_one_axis():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-03T00:00:00Z", "rating": 5},
        {"id": "b", "ts": "2026-10-02T00:00:00Z", "rating": 4},
    ))
    timeline = block["timeline"]
    assert timeline["release_at_utc"] == "2026-10-01T00:00:00Z"
    assert timeline["first_review_at_utc"] == "2026-10-02T00:00:00Z"
    assert timeline["last_review_at_utc"] == "2026-10-03T00:00:00Z"
    assert timeline["observation_at_utc"] == "2026-11-01T00:00:00Z"
    assert timeline["release_to_observation_seconds"] == 31 * 24 * 3600
    kinds = [event["kind"] for event in timeline["events"]]
    assert kinds[0] == "release"
    assert kinds[-1] == "observation"
    at = [event["at_utc"] for event in timeline["events"]]
    assert at == sorted(at)


def test_release_after_observation_is_flagged_not_reordered():
    block = diag(basic(released_at="2026-12-01T00:00:00Z"))
    assert any(w["code"] == "release_after_observation" for w in block["warnings"])
    assert block["status"] == "recovered"
    assert block["timeline"]["release_at_utc"] == "2026-12-01T00:00:00Z"


def test_naive_local_time_uses_declared_timezone():
    block = diag(basic(timezone="Asia/Tokyo", released_at="2026-10-01 09:00:00"))
    release = block["version"]["release"]
    assert release["utc"] == "2026-10-01T00:00:00Z"
    assert release["local"] == "2026-10-01T09:00:00+09:00"
    assert release["offset_seconds"] == 9 * 3600
    assert release["assumed_timezone"] is True
    assert block["timeline"]["timezone"] == "Asia/Tokyo"


def test_explicit_offsets_win_over_declared_timezone():
    block = diag(basic(timezone="America/New_York",
                       released_at="2026-10-01T00:00:00+08:00"))
    release = block["version"]["release"]
    assert release["utc"] == "2026-09-30T16:00:00Z"
    assert release["offset_seconds"] == 8 * 3600
    assert release["assumed_timezone"] is False


@pytest.mark.parametrize("spec, expected_utc", [
    ("UTC+8", "2026-10-01T01:00:00Z"),
    ("+08:00", "2026-10-01T01:00:00Z"),
    (480, "2026-10-01T01:00:00Z"),
    ("Z", "2026-10-01T09:00:00Z"),
])
def test_timezone_spec_variants_resolve_to_same_instant(spec, expected_utc):
    block = diag(basic(timezone=spec, released_at="2026-10-01T09:00:00"))
    assert block["version"]["release"]["utc"] == expected_utc


def test_epoch_seconds_and_millis_agree():
    seconds = diag(basic(released_at=1790000000))["timeline"]["release_at_utc"]
    millis = diag(basic(released_at=1790000000000))["timeline"]["release_at_utc"]
    assert seconds == millis


def test_invalid_timezone_falls_back_and_warns():
    block = diag(basic(timezone="Mars/Olympus", released_at="2026-10-01T00:00:00"))
    assert any(w["code"] == "unknown_timezone" for w in block["warnings"])
    assert block["timeline"]["timezone_source"] == "default"
    assert block["version"]["release"]["utc"] == "2026-10-01T00:00:00Z"


def test_dst_zone_applies_zone_rules_not_fixed_offset():
    block = diag(basic(timezone="America/New_York",
                       released_at="2026-07-01T09:00:00"))
    assert block["version"]["release"]["utc"] == "2026-07-01T13:00:00Z"  # EDT -4
    winter = diag(basic(timezone="America/New_York",
                        released_at="2026-01-01T09:00:00"))
    assert winter["version"]["release"]["utc"] == "2026-01-01T14:00:00Z"  # EST -5


# --------------------------------------------------------------------------- #
# 评分口径与半星精度
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("rating, scale, expected_stars, expected_steps", [
    (4.4, 5, 4.5, 9),        # 4.4 -> 4.5 半星向上
    (4.2, 5, 4.0, 8),        # 4.2 -> 4.0
    (4.25, 5, 4.5, 9),       # 恰好半格向上（ROUND_HALF_UP）
    (8.7, 10, 4.5, 9),       # 十分制 -> 五星
    (87, 100, 4.5, 9),       # 百分制
    (3.75, 5, 4.0, 8),
])
def test_rating_normalizes_across_store_scales(rating, scale, expected_stars, expected_steps):
    block = diag(basic(rating=rating, rating_scale=scale))
    assert block["rating"]["stars"] == expected_stars
    assert block["rating"]["half_steps"] == expected_steps
    assert block["rating"]["raw"] == rating


def test_percent_unit_and_inline_ratio_are_honored():
    assert diag(basic(rating=90, rating_unit="percent"))["rating"]["stars"] == 4.5
    assert diag(basic(rating="18/20"))["rating"]["stars"] == 4.5


def test_nested_rating_object_carries_its_own_scale():
    block = diag(basic(rating={"value": 8.7, "scale": 10}))
    assert block["rating"]["stars"] == 4.5
    assert block["rating"]["raw_scale"] == 10


def test_half_star_distribution_matches_review_count():
    records = [
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 5},
        {"id": "b", "ts": "2026-10-03T00:00:00Z", "rating": 4.5},
        {"id": "c", "ts": "2026-10-04T00:00:00Z", "rating": 8.6, "rating_scale": 10},
    ]
    block = diag(parsing(*records, review_count=3))
    dist = block["star_distribution"]
    assert dist["total"] == 3
    assert sum(dist["counts"].values()) == 3
    assert dist["counts"]["5.0"] == 1
    assert dist["counts"]["4.5"] == 2
    assert block["review_count"]["observed"] == 3


def test_out_of_range_rating_is_clamped_and_warned():
    block = diag(basic(rating=7.5, rating_scale=5))
    assert block["rating"]["stars"] == 5.0
    assert any(w["code"] == "rating_out_of_range" for w in block["warnings"])


def test_zero_rating_with_zero_reviews_means_no_rating():
    block = diag(basic(rating=0, review_count=0))
    assert block["rating"] is None
    assert any(w["code"] == "rating_without_reviews" for w in block["warnings"])


def test_histogram_is_used_when_review_events_absent():
    block = diag(basic(rating_distribution={"5": 10, "4": 30, "4.5": 10}, review_count=50))
    dist = block["star_distribution"]
    assert dist["source"] == "histogram"
    assert dist["total"] == 50
    assert dist["counts"]["4.0"] == 30


# --------------------------------------------------------------------------- #
# 评论数量与重复导入
# --------------------------------------------------------------------------- #
def test_review_count_reconciles_reported_and_observed():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
        {"id": "b", "ts": "2026-10-03T00:00:00Z", "rating": 5},
    ))
    count = block["review_count"]
    assert count["reported"] == 100
    assert count["observed"] == 2
    assert count["basis"] == "individual"
    # 分页抽样（observed < reported）是正常情况，不应告警。
    assert not any(w["code"].startswith("review_count_") for w in block["warnings"])


def test_observed_exceeding_reported_is_flagged():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
        {"id": "b", "ts": "2026-10-03T00:00:00Z", "rating": 5},
        review_count=1,
    ))
    assert any(w["code"] == "review_count_exceeds_reported" for w in block["warnings"])


def test_complete_events_shortfall_is_flagged():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
        review_count=5,
        review_completeness="complete",
    ))
    assert any(w["code"] == "review_count_shortfall" for w in block["warnings"])


def test_mean_mismatch_only_compared_for_full_population():
    partial = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 1},
        review_count=100,
    ))
    assert not any(w["code"] == "mean_rating_mismatch" for w in partial["warnings"])

    full = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 1},
        review_count=1,
    ))
    assert any(w["code"] == "mean_rating_mismatch" for w in full["warnings"])


def test_duplicate_reviews_do_not_inflate_counts_or_distribution():
    item = {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 5}
    block = diag(parsing(item, dict(item), dict(item), review_count=1))
    assert block["review_count"]["observed"] == 1
    assert block["review_count"]["duplicates"] == 2
    assert block["star_distribution"]["total"] == 1
    assert not any(w["code"] == "review_count_mismatch" for w in block["warnings"])


def test_idless_duplicates_deduped_by_content_hash():
    first = {"ts": "2026-10-02T00:00:00Z", "rating": 4, "body": "ok"}
    second = {"ts": "2026-10-05T00:00:00Z", "rating": 5, "body": "different"}
    block = diag(parsing(first, dict(first), second, review_count=2))
    assert block["review_count"]["observed"] == 2
    assert block["review_count"]["duplicates"] == 1


def test_mapping_review_container_is_supported():
    block = diag(basic(reviews={
        "a": {"ts": "2026-10-02T00:00:00Z", "rating": 5},
        "b": {"ts": "2026-10-03T00:00:00Z", "rating": 4},
    }))
    assert block["review_count"]["observed"] == 2


# --------------------------------------------------------------------------- #
# 迟到评论
# --------------------------------------------------------------------------- #
def test_flagged_late_review_still_counts_and_stays_on_timeline():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 5},
        {"id": "late", "ts": "2026-10-10T00:00:00Z", "rating": 3, "late": True},
        review_count=2,
    ))
    assert block["review_count"]["late"] == 1
    assert block["review_count"]["late_ids"] == ["late"]
    assert block["review_count"]["observed"] == 2
    assert block["timeline"]["last_review_at_utc"] == "2026-10-10T00:00:00Z"
    assert block["star_distribution"]["total"] == 2


def test_received_late_beyond_threshold_is_late():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "received_at": "2026-10-30T00:00:00Z",
         "rating": 4},
        review_count=1,
        late_threshold_seconds=3600,
    ))
    assert block["review_count"]["late_ids"] == ["a"]


def test_received_lag_without_threshold_is_not_guessed():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "received_at": "2026-10-30T00:00:00Z",
         "rating": 4},
        review_count=1,
    ))
    assert block["review_count"]["late"] == 0


def test_late_flag_accepts_string_and_integer_forms():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4, "late": "true"},
        {"id": "b", "ts": "2026-10-03T00:00:00Z", "rating": 4, "late": 1},
        {"id": "c", "ts": "2026-10-04T00:00:00Z", "rating": 4, "late": False},
        review_count=3,
    ))
    assert block["review_count"]["late_ids"] == ["a", "b"]


def test_review_after_observation_is_late_but_kept():
    block = diag(parsing(
        {"id": "a", "ts": "2026-12-01T00:00:00Z", "rating": 4},
        review_count=1,
    ))
    assert block["review_count"]["late"] == 1
    assert block["review_count"]["observed"] == 1


# --------------------------------------------------------------------------- #
# 无评论区间
# --------------------------------------------------------------------------- #
def test_explicit_zero_window_becomes_no_review_interval():
    block = diag(basic(
        review_windows=[
            {"start": "2026-10-01T00:00:00Z", "end": "2026-10-05T00:00:00Z",
             "count": 0},
            {"start": "2026-10-05T00:00:00Z", "end": "2026-10-10T00:00:00Z",
             "count": 3, "rating": 4},
        ],
        review_count=3,
    ))
    gaps = block["no_review_windows"]
    assert len(gaps) == 1
    assert gaps[0]["start_utc"] == "2026-10-01T00:00:00Z"
    assert gaps[0]["end_utc"] == "2026-10-05T00:00:00Z"
    assert gaps[0]["basis"] == "explicit_zero_window"
    assert block["gaps_computable"] is True


def test_sparse_reviews_alone_do_not_invent_zero_windows():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 5},
        {"id": "b", "ts": "2026-10-20T00:00:00Z", "rating": 4},
        review_count=2,
    ))
    assert block["no_review_windows"] == []
    assert block["gaps_computable"] is False


def test_complete_event_set_yields_gaps_between_reviews():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 5},
        {"id": "b", "ts": "2026-10-06T00:00:00Z", "rating": 4},
        review_count=2,
        review_completeness="complete",
    ))
    gaps = block["no_review_windows"]
    assert gaps, "complete event set with a gap should expose it"
    covered = [(g["start_utc"], g["end_utc"]) for g in gaps]
    assert ("2026-10-02T00:00:00Z", "2026-10-06T00:00:00Z") in covered
    assert block["gaps_computable"] is True


def test_zero_reviews_zero_windows_yields_full_interval():
    block = diag(basic(rating=0, review_count=0, review_completeness="complete"))
    assert block["rating"] is None
    gaps = block["no_review_windows"]
    assert len(gaps) == 1
    assert gaps[0]["start_utc"] == "2026-10-01T00:00:00Z"
    assert gaps[0]["end_utc"] == "2026-11-01T00:00:00Z"


def test_zero_window_wins_over_derived_gap_without_duplication():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-01T00:00:00Z", "rating": 5},
        {"id": "b", "ts": "2026-10-08T00:00:00Z", "rating": 4},
        review_count=2,
        review_completeness="complete",
        review_windows=[
            {"start": "2026-10-02T00:00:00Z", "end": "2026-10-06T00:00:00Z", "count": 0},
        ],
    ))
    starts = [g["start_utc"] for g in block["no_review_windows"]]
    assert len(starts) == len(set(starts))


# --------------------------------------------------------------------------- #
# 重复导入 / 重入 / 并发
# --------------------------------------------------------------------------- #
def test_reprocessing_own_output_is_stable():
    original = parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 8.6, "rating_scale": 10},
        {"id": "b", "ts": "2026-10-03T00:00:00Z", "rating": 3, "late": True},
        review_count=2, timezone="Asia/Shanghai",
    )
    once = process(original)
    twice = process(once)
    assert twice["_score_align"] == once["_score_align"]
    assert twice["_stage_score_align"] == once["_stage_score_align"] == "naive"


def test_repeated_import_produces_identical_diagnostics():
    record = parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
        review_count=1,
    )
    outputs = [process(copy.deepcopy(record)) for _ in range(5)]
    assert all(out["_score_align"] == outputs[0]["_score_align"] for out in outputs)


def test_concurrent_calls_do_not_share_state():
    record = parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
        {"id": "b", "ts": "2026-10-03T00:00:00Z", "rating": 5, "late": True},
        review_count=2,
    )
    expected = process(record)["_score_align"]

    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        return process(copy.deepcopy(record))["_score_align"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: worker(), range(32)))
    assert all(result == expected for result in results)


def test_input_record_not_mutated_with_nested_structures():
    record = parsing({"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
                     review_count=1)
    snapshot = copy.deepcopy(record)
    process(record)
    assert record == snapshot


# --------------------------------------------------------------------------- #
# 失败恢复
# --------------------------------------------------------------------------- #
def test_bad_field_degrades_to_warning_not_exception():
    block = diag(basic(
        rating="not-a-number",
        review_count="many",
        released_at="not-a-date",
        reviews=[{"id": "a", "ts": "bad", "rating": "x"}, "not-a-dict"],
    ))
    codes = {w["code"] for w in block["warnings"]}
    assert {"invalid_rating", "invalid_review_count", "invalid_timestamp",
            "invalid_review_entry"} <= codes
    assert block["status"] == "recovered"


def test_one_bad_review_does_not_drop_good_reviews():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 5},
        {"id": "bad", "ts": "2026-10-03T00:00:00Z", "rating": "N/A"},
        review_count=2,
    ))
    assert block["review_count"]["observed"] == 2
    assert block["star_distribution"]["total"] == 1  # 只有评分有效的条目进入分布


def test_internal_error_recovers_with_diagnostic(monkeypatch):
    def boom(_record):
        raise RuntimeError("synthetic")

    monkeypatch.setattr(score_align, "_align", boom)
    out = process({"id": "x", "rating": 5})
    block = out["_score_align"]
    assert block["status"] == "recovered"
    assert block["warnings"] == [{"code": "internal_error", "detail": "RuntimeError"}]
    assert out["rating"] == 5  # 原始字段仍在


def test_warnings_are_deterministically_ordered_and_unique():
    block = diag(parsing(
        {"id": "a", "ts": "bad", "rating": "x"},
        {"id": "b", "ts": "worse", "rating": "y"},
        review_count="nope",
    ))
    keys = [(w["code"], w["detail"]) for w in block["warnings"]]
    assert keys == sorted(keys)
    assert len(keys) == len(set(keys))


def test_events_are_serially_json_safe():
    block = diag(parsing(
        {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 4},
        review_count=1,
    ))
    json.dumps(block)  # 不应抛出


def test_randomized_records_preserve_invariants():
    import random
    rng = random.Random(7)

    def make_review():
        return {
            "id": rng.choice([f"r{rng.randint(1, 5)}", None]),
            "ts": f"2026-{rng.randint(9, 11):02d}-{rng.randint(1, 28):02d}"
                  f"T{rng.randint(0, 23):02d}:00:00Z",
            "rating": rng.choice([rng.uniform(0, 5), rng.uniform(0, 10),
                                  rng.uniform(0, 100), "4/5", None]),
            "rating_scale": rng.choice([5, 10, 100, None]),
            "late": rng.choice([True, False, None]),
        }

    for _ in range(200):
        record = {
            "id": "x",
            "rating": rng.uniform(0, 5),
            "review_count": rng.randint(0, 50),
            "released_at": f"2026-{rng.randint(1, 12):02d}-01T00:00:00Z",
            "observed_at": "2026-12-01T00:00:00Z",
            "reviews": [make_review() for _ in range(rng.randint(0, 20))],
        }
        block = diag(record)

        if block["star_distribution"]:
            assert sum(block["star_distribution"]["counts"].values()) == \
                block["star_distribution"]["total"]
            for label, count in block["star_distribution"]["counts"].items():
                assert count >= 0
                assert 0.0 <= float(label) <= 5.0

        timeline = block["timeline"]
        if timeline["first_review_at_utc"] and timeline["last_review_at_utc"]:
            assert timeline["first_review_at_utc"] <= timeline["last_review_at_utc"]

        keys = [(w["code"], w["detail"]) for w in block["warnings"]]
        assert keys == sorted(keys)
        assert len(keys) == len(set(keys))
        json.dumps(block)
        assert diag(process(record)) == block


def test_events_truncation_keeps_release_and_observation_anchors():
    reviews = [
        {"id": f"r{i:04d}", "ts": f"2026-10-{2 + i % 20:02d}T00:00:{i % 60:02d}Z", "rating": 4}
        for i in range(score_align.MAX_EVENTS + 50)
    ]
    block = diag(basic(reviews=reviews, review_count=len(reviews)))
    timeline = block["timeline"]
    assert timeline["events_truncated"] is True
    kinds = {event["kind"] for event in timeline["events"]}
    assert {"release", "observation"} <= kinds
    assert block["review_count"]["observed"] == len(reviews)


def test_blank_optional_fields_behave_like_missing():
    block = diag(basic(rating="", review_count="", released_at=""))
    assert block["rating"] is None
    assert block["review_count"]["reported"] is None
    assert block["timeline"]["release_at_utc"] is None
    assert not any(w["code"].startswith("invalid_") for w in block["warnings"])


def test_version_object_release_timestamp_is_used():
    block = diag(basic(version={"name": "2.0.0", "released_at": "2026-09-15T00:00:00Z"}))
    assert block["version"]["name"] == "2.0.0"
    assert block["timeline"]["release_at_utc"] == "2026-09-15T00:00:00Z"


def test_conflicting_release_times_are_surfaced():
    block = diag(basic(
        released_at="2026-10-01T00:00:00Z",
        version={"name": "2.0.0", "released_at": "2026-09-15T00:00:00Z"},
    ))
    # 版本块更具体，作为时间轴锚点；冲突体现在诊断里。
    assert block["timeline"]["release_at_utc"] == "2026-09-15T00:00:00Z"
    assert any(w["code"] == "release_time_conflict" for w in block["warnings"])


def test_entry_scale_overrides_record_unit():
    block = diag(basic(
        rating_unit="percent",
        reviews=[
            {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 3, "rating_scale": 5},
        ],
        review_count=1,
    ))
    # 条目声明 5 分制分母，不应继承记录级 percent 单位。
    assert block["star_distribution"]["counts"]["3.0"] == 1


def test_entry_unit_overrides_record_scale():
    block = diag(basic(
        rating_scale=10,
        reviews=[
            {"id": "a", "ts": "2026-10-02T00:00:00Z", "rating": 90, "rating_unit": "percent"},
        ],
        review_count=1,
    ))
    # 条目声明百分制，不应继承记录级 10 分制分母。
    assert block["star_distribution"]["counts"]["4.5"] == 1


def test_nested_rating_scope_is_self_consistent():
    by_unit = diag(basic(rating={"value": 90, "unit": "percent"}, rating_scale=10))
    assert by_unit["rating"]["unit"] == "percent"
    assert by_unit["rating"]["raw_scale"] == 100
    by_scale = diag(basic(rating={"value": 3, "scale": 5}, rating_unit="percent"))
    assert by_scale["rating"]["unit"] == "stars"
    assert by_scale["rating"]["raw_scale"] == 5


def test_histogram_keys_tolerate_labels_and_commas():
    block = diag(basic(rating_distribution={"5 stars": 2, "4.0": 3, "3": "1"}, review_count=6))
    dist = block["star_distribution"]
    assert dist["source"] == "histogram"
    assert dist["counts"]["5.0"] == 2
    assert dist["counts"]["4.0"] == 3
    assert dist["counts"]["3.0"] == 1


def test_native_datetime_and_date_inputs_are_accepted():
    import datetime as dt
    block = diag(basic(
        released_at=dt.datetime(2026, 10, 1, 9, 0, tzinfo=dt.timezone(dt.timedelta(hours=2))),
        observed_at=dt.date(2026, 11, 1),
        timezone="Asia/Shanghai",
    ))
    assert block["timeline"]["release_at_utc"] == "2026-10-01T07:00:00Z"
    assert block["timeline"]["observation_at_utc"] == "2026-10-31T16:00:00Z"
