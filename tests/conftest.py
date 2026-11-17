"""review_page 离线夹具与测试共用工具。

夹具均为项目自有离线数据（无网络输入），集中放在 tests 下管理。
"""
import json
from pathlib import Path

import pytest

FIX_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "review_pages"
SCENARIOS = ["first_import", "duplicate_batch", "missing_fields",
             "out_of_order", "recovery"]


def load_fixture(name):
    with open(FIX_DIR / f"{name}.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def clean_state():
    """每个用例独立的 review_page 会话状态，避免进程内串扰。"""
    from appvitals import review_page
    review_page.reset_state()
    yield review_page
    review_page.reset_state()


def replay(module, envelopes, state=None):
    """逐页回放，返回 (落库记录列表, receipt 列表)。"""
    records, receipts = [], []
    for env in envelopes:
        receipt = module.process_page(env, state=state)
        receipts.append(receipt)
        records.extend(receipt["records"])
    return records, receipts


def index_by_id(records):
    return {r["id"]: r for r in records}
