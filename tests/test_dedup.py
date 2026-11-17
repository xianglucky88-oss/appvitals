import json
import tempfile
import unittest
from pathlib import Path

from src.appvitals import config, dedup, identity, keyer, store
from src.appvitals.models import Record
from src.appvitals.dedup import (
    NEW, RETRANSMIT, UPDATE, ROLLBACK, DELETE, SUPPRESSED, STALE, QUARANTINED,
    DedupRetryable,
)


class DedupCase(unittest.TestCase):
    def setUp(self):
        dedup.reset()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = str(Path(self._tmpdir.name) / "dedup_test.db")
        self._orig_db = config.DB_PATH
        config.DB_PATH = Path(self._db_path)
        self.con = store.connect()
        dedup.bind(self.con)

    def tearDown(self):
        dedup.reset()
        config.DB_PATH = self._orig_db
        self._tmpdir.cleanup()

    # 模拟 pipeline 中 dedup → keyer → store 的落库段
    def feed(self, rec):
        out = dedup.process(rec)
        if out is not None:
            store.upsert(self.con, Record(key=keyer.key_of(out), data=out))
        self.con.commit()
        return out

    @staticmethod
    def review(rid, **over):
        base = {"kind": "review", "app_id": "app-1", "review_id": rid, "src": "api",
                "rating": 5, "title": "t", "text": "nice", "ts": "2026-11-10T00:00:00Z"}
        base.update(over)
        return base

    @staticmethod
    def version(ver, **over):
        base = {"kind": "version", "app_id": "app-1", "version": ver, "src": "official",
                "release_notes": "n", "channel": "prod", "ts": "2026-11-10T00:00:00Z"}
        base.update(over)
        return base

    @staticmethod
    def score(**over):
        base = {"kind": "score", "app_id": "app-1", "country": "us", "src": "official",
                "rating_avg": 4.2, "rating_count": 100, "ts": "2026-11-10T00:00:00Z"}
        base.update(over)
        return base


class FingerprintTests(DedupCase):
    def test_identity_stable_across_content_and_src(self):
        a = self.review("rv1", text="old", src="api")
        b = self.review("rv1", text="new", src="web")
        self.assertEqual(identity.identity_fp(a), identity.identity_fp(b))
        self.assertNotEqual(identity.content_fp(a), identity.content_fp(b))

    def test_distinct_identities_distinct_fingerprints(self):
        self.assertNotEqual(
            identity.identity_fp(self.review("rv1")),
            identity.identity_fp(self.review("rv2")))
        self.assertNotEqual(
            identity.identity_fp(self.version("1.0")),
            identity.identity_fp(self.version("1.1")))

    def test_same_version_different_build_is_distinct_identity(self):
        a = self.version("1.0", build="100")
        b = self.version("1.0", build="101")
        self.assertNotEqual(identity.identity_fp(a), identity.identity_fp(b))

    def test_partial_missing_content_fields_still_fingerprints(self):
        full = self.review("rv1")
        partial = self.review("rv1")
        del partial["text"]
        del partial["title"]
        # 身份不变；内容指纹随可得字段变化但稳定可重复
        self.assertEqual(identity.identity_fp(full), identity.identity_fp(partial))
        self.assertEqual(identity.content_fp(partial), identity.content_fp(partial))

    def test_unknown_kind_is_identity_error(self):
        with self.assertRaises(IdentityError):
            identity.identity_fp({"kind": "mystery", "app_id": "a"})


IdentityError = identity.IdentityError


class FourWayClassificationTests(DedupCase):
    # ---- 重传 ----
    def test_retransmit_is_absorbed_and_no_row(self):
        r = self.review("rv1")
        first = self.feed(r)
        rows_after_first = store.count(self.con)
        again = self.feed(dict(r))
        self.assertEqual(first["_stage_dedup"], NEW)
        self.assertIsNone(again)
        self.assertEqual(store.count(self.con), rows_after_first)
        self.assertEqual(dedup.stats()[RETRANSMIT], 1)

    # ---- 评论编辑 ----
    def test_review_edit_updates_same_row(self):
        first = self.feed(self.review("rv1", text="v1"))
        edited = self.feed(self.review("rv1", text="v2 edited",
                                       ts="2026-11-11T00:00:00Z"))
        self.assertEqual(edited["_stage_dedup"], UPDATE)
        self.assertEqual(edited["_identity_fp"], first["_identity_fp"])
        self.assertEqual(keyer.key_of(edited), keyer.key_of(first))
        self.assertEqual(store.count(self.con), 1)  # 无幽灵行

    # ---- 评论删除与粘性墓碑 ----
    def test_review_delete_tombstone_is_sticky(self):
        self.feed(self.review("rv1"))
        deleted = self.feed(self.review("rv1", op="delete",
                                        ts="2026-11-12T00:00:00Z"))
        self.assertEqual(deleted["_stage_dedup"], DELETE)
        self.assertTrue(deleted["_tombstone"])
        self.assertEqual(store.count(self.con), 1)
        idx = store.index_get(self.con, deleted["_identity_fp"])
        self.assertEqual(idx["is_tombstone"], 1)

        # 同一删除重放 => 重传吸收
        replay = self.feed(self.review("rv1", op="delete",
                                       ts="2026-11-12T00:00:00Z"))
        self.assertIsNone(replay)

        # 同优先级旧消息不能复活墓碑；权威来源的新编辑可以复活
        blocked = self.feed(self.review("rv1", text="zombie", src="web",
                                        ts="2026-11-13T00:00:00Z"))
        self.assertIsNone(blocked)
        revived = self.feed(self.review("rv1", text="official edit", src="official",
                                        ts="2026-11-14T00:00:00Z"))
        self.assertEqual(revived["_stage_dedup"], UPDATE)
        self.assertEqual(store.count(self.con), 1)

    def test_delete_first_seen_creates_tombstone_row(self):
        out = self.feed(self.review("rv9", op="delete"))
        self.assertEqual(out["_stage_dedup"], DELETE)
        self.assertEqual(store.count(self.con), 1)

    # ---- 版本回滚 ----
    def test_version_rollback_distinguished_from_new_and_edit(self):
        v1 = self.feed(self.version("1.0", release_notes="first",
                                    ts="2026-11-01T00:00:00Z"))
        v2 = self.feed(self.version("1.1", release_notes="second",
                                    ts="2026-11-02T00:00:00Z"))
        rollback = self.feed(self.version("1.1", op="rollback", release_notes="first",
                                          ts="2026-11-03T00:00:00Z"))
        self.assertEqual(v1["_stage_dedup"], NEW)
        self.assertEqual(v2["_stage_dedup"], NEW)  # 新版本号 = 真正新记录
        self.assertEqual(rollback["_stage_dedup"], ROLLBACK)
        # 回滚不新增身份行（1.1 身份仍是同一存储键）
        self.assertEqual(keyer.key_of(rollback), keyer.key_of(v2))
        self.assertEqual(store.count(self.con), 2)
        # 回滚后同样内容重放仍是重传，不会再触发一次 rollback
        self.assertIsNone(self.feed(self.version("1.1", op="rollback", release_notes="first",
                                                 ts="2026-11-03T00:00:00Z")))

    # ---- 评分刷新与真正新记录 ----
    def test_score_refresh_vs_new_score_identity(self):
        s1 = self.feed(self.score(rating_avg=4.0))
        s2 = self.feed(self.score(rating_avg=4.5, ts="2026-11-11T00:00:00Z"))
        s_other_country = self.feed(self.score(country="jp", rating_avg=3.9))
        self.assertEqual(s1["_stage_dedup"], NEW)
        self.assertEqual(s2["_stage_dedup"], UPDATE)
        self.assertEqual(s_other_country["_stage_dedup"], NEW)
        self.assertEqual(keyer.key_of(s1), keyer.key_of(s2))
        self.assertNotEqual(keyer.key_of(s1), keyer.key_of(s_other_country))
        self.assertEqual(store.count(self.con), 2)


class SourcePriorityTests(DedupCase):
    def test_lower_priority_source_suppressed(self):
        self.feed(self.review("rv1", src="official", text="authoritative"))
        out = self.feed(self.review("rv1", src="rpa", text="scraped rewrite",
                                    ts="2026-11-11T00:00:00Z"))
        self.assertIsNone(out)
        self.assertEqual(dedup.stats()[SUPPRESSED], 1)
        # 权威源后续编辑仍然采纳
        ok = self.feed(self.review("rv1", src="official", text="authoritative v2",
                                   ts="2026-11-12T00:00:00Z"))
        self.assertEqual(ok["_stage_dedup"], UPDATE)

    def test_equal_priority_tie_is_deterministic(self):
        # alpha/beta 同档（rank 3 vs 4 不同档），这里用两个未知源同为 99 档验证决胜
        a = self.feed(self.review("rv1", src="zzz", text="z"))
        b = self.feed(self.review("rv1", src="aaa", text="a",
                                  ts="2026-11-11T00:00:00Z"))
        self.assertEqual(a["_stage_dedup"], NEW)
        # aaa < zzz => aaa 字典序靠前，应采纳
        self.assertEqual(b["_stage_dedup"], UPDATE)
        # 反向顺序结果一致：在全新库上以相反顺序重放，最终状态不取决于谁先到
        dedup.reset()
        second_db = Path(self._tmpdir.name) / "dedup_reverse.db"
        config.DB_PATH = second_db
        try:
            con2 = store.connect()
            dedup.bind(con2)
            b2 = dedup.process(self.review("rv1", src="aaa", text="a",
                                           ts="2026-11-11T00:00:00Z"))
            store.upsert(con2, Record(key=keyer.key_of(b2), data=b2))
            a2 = dedup.process(self.review("rv1", src="zzz", text="z"))
            self.assertEqual(b2["_stage_dedup"], NEW)
            self.assertIsNone(a2)  # zzz 字典序靠后，被确定性抑制
            # 反向场景的最终内容与正向场景一致：aaa 的内容胜出
            _, only = store.all_records(con2)[0]
            self.assertEqual(only["text"], "a")
        finally:
            dedup.reset()
            config.DB_PATH = Path(self._db_path)
            dedup.bind(self.con)


class OutOfOrderTests(DedupCase):
    def test_late_older_event_marked_stale_and_not_applied(self):
        newer = self.feed(self.review("rv1", text="new",
                                      ts="2026-11-15T00:00:00Z"))
        late = self.feed(self.review("rv1", text="old content arriving late",
                                     ts="2026-11-01T00:00:00Z"))
        self.assertEqual(newer["_stage_dedup"], NEW)
        self.assertIsNone(late)
        self.assertEqual(dedup.stats()[STALE], 1)
        key, data = store.all_records(self.con)[0]
        self.assertEqual(data["text"], "new")  # 最终状态未被旧消息覆盖

    def test_final_state_independent_of_arrival_order(self):
        seq = [self.review("rv1", text=t, ts=f"2026-11-{day:02d}T00:00:00Z")
               for day, t in ((1, "d1"), (2, "d2"), (3, "d3"))]
        for rec in reversed(seq):  # 乱序：3 -> 2 -> 1
            self.feed(dict(rec))
        rows = dict(store.all_records(self.con))
        self.assertEqual(len(rows), 1)
        only = next(iter(rows.values()))
        self.assertEqual(only["text"], "d3")  # 最新事件内容胜出
        self.assertEqual(dedup.stats()[STALE], 2)

    def test_missing_ts_does_not_block_processing(self):
        r = self.review("rv1")
        del r["ts"]
        out = self.feed(r)
        self.assertEqual(out["_stage_dedup"], NEW)


class MissingFieldTests(DedupCase):
    def test_missing_identity_field_quarantined_not_stored(self):
        bad = self.review("rv1")
        del bad["app_id"]
        result = self.feed(bad)
        self.assertIsNone(result)
        self.assertEqual(dedup.stats()[QUARANTINED], 1)
        self.assertEqual(store.count(self.con), 0)  # 幽灵记录零容忍
        q = store.quarantine_rows(self.con)
        self.assertEqual(q[0][1], "missing_identity_field")

    def test_bad_timestamp_quarantined(self):
        bad = self.review("rv1", ts="not-a-timestamp")
        self.assertIsNone(self.feed(bad))
        reasons = [r[1] for r in store.quarantine_rows(self.con)]
        self.assertIn("bad_timestamp", reasons)

    def test_bad_numeric_quarantined(self):
        bad = self.score(rating_avg="high")
        self.assertIsNone(self.feed(bad))
        reasons = [r[1] for r in store.quarantine_rows(self.con)]
        self.assertIn("bad_numeric", reasons)

    def test_quarantine_is_idempotent(self):
        bad = self.review("rv1")
        del bad["review_id"]
        self.feed(dict(bad))
        self.feed(dict(bad))
        self.assertEqual(len(store.quarantine_rows(self.con)), 1)


class RestartAndReplayTests(DedupCase):
    def test_state_survives_restart(self):
        self.feed(self.review("rv1", text="v1"))
        # 模拟进程重启：释放并重建连接与状态机，状态全部从 sqlite 恢复
        dedup.reset()
        new_con = store.connect()
        dedup.bind(new_con)
        self.con = new_con
        replay = self.feed(self.review("rv1", text="v1"))
        self.assertIsNone(replay)
        self.assertEqual(dedup.stats()[RETRANSMIT], 1)
        edit = self.feed(self.review("rv1", text="v2",
                                     ts="2026-11-20T00:00:00Z"))
        self.assertEqual(edit["_stage_dedup"], UPDATE)
        self.assertEqual(store.count(new_con), 1)

    def test_full_replay_final_state_unchanged(self):
        batch = [self.review("rv1", text="a"),
                 self.review("rv1", text="b", ts="2026-11-11T00:00:00Z"),
                 self.version("2.0")]
        for r in batch:
            self.feed(dict(r))
        rows_once = store.all_records(self.con)
        audit_once = len(store.audit_rows(self.con))
        for r in batch:  # 完整重复回放
            self.feed(dict(r))
        self.assertEqual(store.all_records(self.con), rows_once)
        self.assertEqual(store.count(self.con), len(rows_once))
        self.assertGreaterEqual(len(store.audit_rows(self.con)), audit_once)


class RuleUpgradeTests(DedupCase):
    def test_rule_upgrade_rebaselines_without_duplicates(self):
        out = self.feed(self.review("rv1", text="v1"))
        idfp = out["_identity_fp"]
        before = store.index_get(self.con, idfp)
        self.assertEqual(before["rule_version"], identity.FP_RULE_VERSION)

        # 模拟规则升级：提升版本号 + 篡改库里的旧 content_fp
        old_version = identity.FP_RULE_VERSION
        new_version = old_version + 1
        identity.FP_RULE_VERSION = new_version
        dedup.FP_RULE_VERSION = new_version
        try:
            self.con.execute("UPDATE dedup_index SET content_fp='cf0:legacy', rule_version=0")
            self.con.commit()
            dedup.rebaseline_for_rule_version()
            after = store.index_get(self.con, idfp)
            self.assertEqual(after["rule_version"], new_version)
            self.assertEqual(after["content_fp"], identity.content_fp(out))
            self.assertEqual(after["identity_fp"], before["identity_fp"])  # 身份键不动
            row_count = store.count(self.con)
            # 升级后重放 => 重传吸收，而不是 new
            self.assertIsNone(self.feed(self.review("rv1", text="v1")))
            self.assertEqual(store.count(self.con), row_count)
        finally:
            identity.FP_RULE_VERSION = old_version
            dedup.FP_RULE_VERSION = old_version


class MigrationTests(DedupCase):
    def test_migrate_legacy_anchors_existing_rows_without_copying(self):
        # 构造一条历史 records 行（无 kind、无 dedup 状态），模拟改造前落库数据
        legacy = {"id": "leg-1", "src": "alpha", "name": "Old", "value": 7,
                  "status": "active", "ts": "2026-10-01T00:00:00Z"}
        store.upsert(self.con, Record(key=keyer.key_of(legacy), data=legacy))
        self.con.commit()
        rows_before = store.count(self.con)

        anchored = dedup.migrate_legacy()
        self.assertEqual(anchored, 1)
        self.assertEqual(store.count(self.con), rows_before)  # 不复制行

        # 迁移幂等
        self.assertEqual(dedup.migrate_legacy(), 0)

        # 迁移后同一历史记录重放 => 重传
        self.assertIsNone(self.feed(dict(legacy)))
        self.assertEqual(dedup.stats()[RETRANSMIT], 1)

    def test_migrate_skips_unidentifiable_rows_to_quarantine(self):
        bad = {"src": "alpha", "name": "no id"}  # event 缺 id
        store.upsert(self.con, Record(key="fallbackkey", data=bad))
        self.con.commit()
        self.assertEqual(dedup.migrate_legacy(), 0)
        self.assertEqual(len(store.quarantine_rows(self.con)), 1)


class RetryPathTests(DedupCase):
    def test_retryable_operational_error_raises_dedupretryable(self):
        target = self.review("rv1")
        idfp = identity.identity_fp(target)
        real_index_get = store.index_get

        def flaky(con, fp):
            if fp == idfp:
                raise store.sqlite3.OperationalError("database is locked")
            return real_index_get(con, fp)

        store.index_get = flaky
        try:
            with self.assertRaises(DedupRetryable):
                dedup.process(target)
        finally:
            store.index_get = real_index_get
        # savepoint 回滚：失败记录不留任何半条状态
        self.assertIsNone(store.index_get(self.con, idfp))
        self.assertEqual(dedup.stats()["retry"], 1)
        # 瞬时故障恢复后同一条记录可正常处理（可重试语义）
        ok = dedup.process(target)
        self.assertEqual(ok["_stage_dedup"], NEW)

    def test_pipeline_retries_then_succeeds(self):
        from src.appvitals import pipeline
        calls = {"n": 0}
        real_index_get = store.index_get

        def flaky_then_ok(con, fp):
            calls["n"] += 1
            if calls["n"] == 1:
                raise store.sqlite3.OperationalError("database is locked")
            return real_index_get(con, fp)

        store.index_get = flaky_then_ok
        try:
            result = pipeline.run(self.con)  # 走真实 happy-path 夹具
            self.assertEqual(result["fed"], 4)
            self.assertEqual(result["rows"], 3)
            self.assertGreaterEqual(result["retried"], 1)
        finally:
            store.index_get = real_index_get


class SensitiveDataTests(DedupCase):
    def test_diagnostics_and_quarantine_never_contain_raw_values(self):
        secret = "super-secret-token-value-123"
        rec = self.review("rv1", token=secret, email="a@b.example", text="public-text")
        self.feed(rec)
        bad = self.review("rv2", token=secret)
        del bad["app_id"]
        self.feed(bad)

        blob = json.dumps({
            "ring": dedup.diagnostics(),
            "quarantine": store.quarantine_rows(self.con),
            "audit": store.audit_rows(self.con),
        }, ensure_ascii=False)
        self.assertNotIn(secret, blob)
        self.assertNotIn("a@b.example", blob)
        self.assertNotIn("public-text", blob)
        # 但仍然可诊断：键名与类型保留
        self.assertIn("redacted", blob)

    def test_redacted_preview_shapes_only(self):
        preview = identity.redacted_preview({"token": "x", "rating": 5})
        self.assertNotIn("x", preview)
        self.assertIn("token", preview)


class ChainIntegrationTests(DedupCase):
    def test_dedup_sits_in_full_chain_and_classifies(self):
        # 端到端：loader 夹具 → 全阶段链 → store，四分类决策出现在最终注解里
        from src.appvitals import pipeline
        result = pipeline.run(self.con)
        self.assertEqual(result["fed"], 4)
        self.assertEqual(result["rows"], 3)
        decisions = {d[2] for d in store.audit_rows(self.con)}
        self.assertIn(NEW, decisions)
        self.assertIn(RETRANSMIT, decisions)
        # 每条落库记录都带身份指纹；typed 身份键 == key，event 老记录保留基线 id 键
        for key, data in store.all_records(self.con):
            self.assertIn("_identity_fp", data)
            if data.get("kind"):
                self.assertEqual(key, data["_identity_fp"])
            else:
                self.assertEqual(key, data["id"])


if __name__ == "__main__":
    unittest.main()
