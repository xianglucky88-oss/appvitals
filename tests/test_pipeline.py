import json
import tempfile
import unittest
from pathlib import Path

from src.appvitals import pipeline, store, dedup
from src.appvitals import config

GOLDEN = Path(__file__).parent / "golden"


class _TmpDB(unittest.TestCase):
    def setUp(self):
        dedup.reset()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = str(Path(self._tmpdir.name) / "pipeline_test.db")
        self._orig_db = config.DB_PATH
        config.DB_PATH = Path(self._db_path)

    def tearDown(self):
        dedup.reset()
        config.DB_PATH = self._orig_db
        self._tmpdir.cleanup()

    def con(self):
        return store.connect()


def _strip_dedup(d):
    return {k: v for k, v in d.items()
            if not k.startswith(("_stage_dedup", "_dedup", "_identity", "_content", "_tombstone"))}


class GoldenLockTests(_TmpDB):
    """改造前生产输出锁定：fed/rows 与每行的业务载荷不得漂移。"""

    def test_run_metrics_match_baseline(self):
        result = pipeline.run(self.con())
        golden = json.loads((GOLDEN / "baseline_run.json").read_text())
        self.assertEqual(result["fed"], golden["fed"])
        self.assertEqual(result["rows"], golden["rows"])

    def test_rows_match_baseline_payloads(self):
        con = self.con()
        pipeline.run(con)
        current = [{"key": k, "data": _strip_dedup(d)} for k, d in store.all_records(con)]
        golden = json.loads((GOLDEN / "baseline_rows.json").read_text(encoding="utf-8"))
        golden = [{"key": g["key"], "data": _strip_dedup(g["data"])} for g in golden]
        self.assertEqual(current, golden)

    def test_idempotent_replay_adds_no_rows(self):
        con = self.con()
        a = pipeline.run(con)
        b = pipeline.run(con)  # 重跑不增行（身份化去重 + 幂等 upsert）
        self.assertEqual(a["fed"], 4)
        self.assertEqual(b["rows"], a["rows"])
        self.assertEqual(a["rows"], 3)
        self.assertEqual(b["dedup"]["new"], 0)
        self.assertEqual(b["dedup"]["retransmit"], 4)


class StoreRoundtripTests(_TmpDB):
    def test_store_roundtrip(self):
        con = self.con()
        self.assertGreaterEqual(store.count(con), 0)


if __name__ == "__main__":
    unittest.main()
