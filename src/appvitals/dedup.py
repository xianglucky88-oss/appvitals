"""单一职责：版本 / 评分 / 评论记录的重复判定与状态化去重。

作为既有生产步骤改造：`process(record)` 签名与 happy-path 位置不变，内部从
"贴阶段标记" 升级为有持久状态的决策引擎。

四类必须区分的语义（验收口径）：
- retransmit 同一事件重传：身份+内容完全相同 => 不落新行，仅计数。
- 评论编辑/删除：身份相同、内容不同 => 同一存储键上更新；删除置墓碑（粘性）。
- 版本回滚：op=rollback 显式标记，身份不变、内容回到历史 => 同一行上的 rollback 决策。
- 真正新记录：首次出现的身份 => 新行。

配套保证：
- 乱序批次：晚到的旧消息标 stale 且绝不覆盖较新状态；最终状态与到达顺序无关。
- 部分字段缺失：身份字段缺失 => quarantine（不可重试）；内容字段缺失 => 按可得字段算指纹。
- 重复运行：所有决策幂等，重放不增行。
- 历史迁移：migrate_legacy() 把 records 老行锚定进 dedup_index，不复制、不新增行。
- 规则升级：FP_RULE_VERSION 抬升后重建内容指纹基线，身份键不动 => 不产生重复。
- 敏感原始数据不进日志：诊断只保留指纹、决策码与 redacted_preview（键名+类型）。
"""
from __future__ import annotations

import hashlib
import json
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

from . import identity as _id
from . import store as _store
from .identity import (
    FP_RULE_VERSION, IdentityError, composite_fingerprint, redacted_preview,
    source_rank,
)

# 决策码（稳定契约，测试与审计依赖这些字面量）
NEW = "new"
RETRANSMIT = "retransmit"
UPDATE = "update"
ROLLBACK = "rollback"
DELETE = "delete"
SUPPRESSED = "suppressed"   # 来源优先级不足，不采纳
STALE = "stale"             # 乱序晚到，不采纳
QUARANTINED = "quarantined"  # 不可重试坏记录，隔离不落库

# 决策 => 是否仍需写入 records 表（None 表示 pipeline 跳过落库，物理上杜绝幽灵行）
STORE_WORTHY = {NEW, UPDATE, ROLLBACK, DELETE}


class DedupRetryable(RuntimeError):
    """可重试的瞬时故障（如 sqlite 锁）。调用方应重试整条记录，而非丢弃。"""


# 数值型内容字段：出现且无法解析为数字 => 不可重试数据缺陷
_NUMERIC_FIELDS = {
    "review": ("rating", "helpful"),
    "score": ("rating_avg", "rating_count", "star1", "star2", "star3", "star4", "star5"),
    "event": ("value",),
    "version": (),
}


class _DedupState:
    def __init__(self, con, owns: bool):
        self.con = con
        self.owns = owns
        self.stats = {k: 0 for k in (
            NEW, RETRANSMIT, UPDATE, ROLLBACK, DELETE, SUPPRESSED, STALE,
            QUARANTINED, "retry")}
        self.ring = deque(maxlen=200)

    def close(self):
        if self.owns:
            self.con.close()


_state: Optional[_DedupState] = None


# ---------------------------------------------------------------- 生命周期

def bind(con) -> None:
    """由 pipeline 在运行开始时绑定（通常与 pipeline 共用同一连接）。"""
    global _state
    if _state is not None and _state.owns:
        _state.close()
    _state = _DedupState(con, owns=False)
    _ensure_rule_version(_state)


def reset() -> None:
    """测试/进程级清理：释放自持连接。"""
    global _state
    if _state is not None and _state.owns:
        _state.close()
    _state = None


def _state_or_default() -> _DedupState:
    global _state
    if _state is None:
        _state = _DedupState(_store.connect(), owns=True)
        _ensure_rule_version(_state)
    return _state


def flush() -> dict[str, int]:
    """提交本批次所有状态变更并返回计数快照。"""
    st = _state_or_default()
    st.con.commit()
    return dict(st.stats)


def stats() -> dict[str, int]:
    return dict(_state_or_default().stats)


def diagnostics() -> list[dict[str, Any]]:
    """安全诊断视图：仅含指纹/决策码/字段形状，任何原始敏感值都不在其中。"""
    return [dict(e) for e in _state_or_default().ring]


# ---------------------------------------------------------------- 规则版本

def _ensure_rule_version(st: _DedupState) -> None:
    raw = _store.meta_get(st.con, "fp_rule_version")
    if raw is None:
        _store.meta_set(st.con, "fp_rule_version", FP_RULE_VERSION)
        st.con.commit()
        return
    if int(raw) < FP_RULE_VERSION:
        rebaseline_for_rule_version()


def rebaseline_for_rule_version() -> None:
    """规则升级：按新规则重建每条已知身份的内容指纹基线。

    身份指纹与存储键完全不动，只刷新 content_fp/rule_version，
    因此升级后的第一次重放会被判 retransmit 而非 new（无幽灵行）。
    """
    st = _state_or_default()
    current = int(_store.meta_get(st.con, "fp_rule_version", "0"))
    if current >= FP_RULE_VERSION:
        return
    by_key = {k: d for k, d in _store.all_records(st.con)}
    for row in _store.index_rows(st.con):
        identity_fp, kind, store_key = row[0], row[1], row[2]
        rec = by_key.get(store_key)
        if rec is None:
            continue
        new_cfp = _id.content_fp(rec)
        _store.index_reanchor(st.con, identity_fp=identity_fp,
                              content_fp=new_cfp, rule_version=FP_RULE_VERSION)
        _store.audit_append(st.con, identity_fp=identity_fp, content_fp=new_cfp,
                            decision="rule_upgrade", reason=f"v{current}->v{FP_RULE_VERSION}",
                            src=None, rule_version=FP_RULE_VERSION, ts=_now_iso())
    _store.meta_set(st.con, "fp_rule_version", FP_RULE_VERSION)
    st.con.commit()


# ---------------------------------------------------------------- 历史迁移

def migrate_legacy() -> int:
    """把 records 表中尚未登记身份的历史行锚定进 dedup_index。

    幂等：store 层 anti-join 保证重复执行只处理新出现的老行；不插入 records。
    """
    st = _state_or_default()
    anchored = 0
    for key, data in _store.legacy_records(st.con):
        rec = json.loads(data)
        try:
            idfp, cfp = composite_fingerprint(rec)
            kind = str(rec.get("kind") or "event").strip().lower() or "event"
        except IdentityError:
            _quarantine(st, rec, "legacy_identity_invalid")
            continue
        ts, _ = _parse_ts(rec.get("ts"))
        _store.index_upsert(
            st.con, identity_fp=idfp, kind=kind, store_key=key, content_fp=cfp,
            src=str(rec.get("src") or ""), src_rank=source_rank(rec.get("src")),
            event_ts=ts, is_tombstone=False, rule_version=FP_RULE_VERSION,
            updated_at=_now_iso())
        _store.audit_append(st.con, identity_fp=idfp, content_fp=cfp,
                            decision="migrated", reason="legacy_anchor",
                            src=rec.get("src"), rule_version=FP_RULE_VERSION, ts=_now_iso())
        anchored += 1
    st.con.commit()
    return anchored


# ---------------------------------------------------------------- 主入口

def process(record):
    """对单条记录做去重决策。

    返回：
    - 注解后的记录（dict）：该身份需要在 records 表 upsert（new/update/rollback/delete）。
    - None：该记录被吸收（重传/抑制/过期/隔离），pipeline 必须跳过落库。
    抛出 DedupRetryable 表示可重试瞬时故障；数据缺陷不抛、直接 quarantine。
    """
    st = _state_or_default()
    rec = dict(record)

    # --- 不可重试路径：身份/字段缺陷 => 隔离，不入库，不产生幽灵行 ---
    try:
        idfp, cfp = composite_fingerprint(rec)
        _validate_content(rec)
        ts, ts_cmp = _parse_ts(rec.get("ts"))
    except IdentityError as exc:
        _quarantine(st, rec, exc.reason)
        return None

    kind = str(rec.get("kind") or "event").strip().lower() or "event"
    src = str(rec.get("src") or "").strip().lower()
    rank = source_rank(src)
    is_delete = rec.get("op") in ("delete", "remove") or rec.get("deleted") is True
    is_rollback = kind == "version" and rec.get("op") == "rollback"

    # --- 可重试路径：状态读写故障包在 SAVEPOINT 里，失败可整记录重试 ---
    try:
        st.con.execute("SAVEPOINT dedup_decision")
        existing = _store.index_get(st.con, idfp)

        if existing is not None and _is_exact_retransmit(existing, cfp, is_delete):
            decision, reason = RETRANSMIT, "identical_replay"
            _store.audit_append(st.con, identity_fp=idfp, content_fp=cfp,
                                decision=decision, reason=reason, src=src,
                                rule_version=FP_RULE_VERSION, ts=_now_iso())
            st.con.execute("RELEASE SAVEPOINT dedup_decision")
            return _absorb(st, decision, reason, idfp, cfp, src, rec)

        if existing is not None and _lower_priority(existing, rank, src):
            decision, reason = SUPPRESSED, "lower_source_priority"
            _store.audit_append(st.con, identity_fp=idfp, content_fp=cfp,
                                decision=decision, reason=reason, src=src,
                                rule_version=FP_RULE_VERSION, ts=_now_iso())
            st.con.execute("RELEASE SAVEPOINT dedup_decision")
            return _absorb(st, decision, reason, idfp, cfp, src, rec)

        if existing is not None and _is_stale(existing, ts_cmp):
            decision, reason = STALE, "late_older_event_ts"
            _store.audit_append(st.con, identity_fp=idfp, content_fp=cfp,
                                decision=decision, reason=reason, src=src,
                                rule_version=FP_RULE_VERSION, ts=_now_iso())
            st.con.execute("RELEASE SAVEPOINT dedup_decision")
            return _absorb(st, decision, reason, idfp, cfp, src, rec)

        if existing is None:
            if is_delete:
                decision, reason = DELETE, "tombstone_first_seen"
            elif is_rollback:
                decision, reason = ROLLBACK, "rollback_first_seen"
            else:
                decision, reason = NEW, "first_identity"
        else:
            if is_delete:
                decision, reason = DELETE, "tombstone_replace"
            elif existing["is_tombstone"]:
                decision, reason = UPDATE, "resurrect_after_delete"
            elif is_rollback:
                decision, reason = ROLLBACK, "rollback_revision"
            else:
                decision, reason = UPDATE, _edit_reason(kind)

        _store.index_upsert(
            st.con, identity_fp=idfp, kind=kind,
            store_key=_store_key(rec, idfp), content_fp=cfp, src=src, src_rank=rank,
            event_ts=ts, is_tombstone=is_delete, rule_version=FP_RULE_VERSION,
            updated_at=_now_iso())
        _store.audit_append(st.con, identity_fp=idfp, content_fp=cfp,
                            decision=decision, reason=reason, src=src,
                            rule_version=FP_RULE_VERSION, ts=_now_iso())
        st.con.execute("RELEASE SAVEPOINT dedup_decision")
    except _store.sqlite3.OperationalError as exc:
        st.con.execute("ROLLBACK TO SAVEPOINT dedup_decision")
        st.stats["retry"] += 1
        # 诊断里只放异常类型，不放可能含参数值的异常全文
        st.ring.append({"decision": "retryable_error", "reason": type(exc).__name__,
                        "identity_fp": idfp, "src": src})
        raise DedupRetryable(type(exc).__name__) from None

    st.stats[decision] += 1
    st.ring.append({"decision": decision, "reason": reason, "identity_fp": idfp,
                    "content_fp": cfp, "src": src, "kind": kind,
                    "preview": redacted_preview(rec)})
    rec["_stage_dedup"] = decision
    rec["_dedup_reason"] = reason
    rec["_identity_fp"] = idfp
    rec["_content_fp"] = cfp
    if is_delete:
        rec["_tombstone"] = True
    return rec


# ---------------------------------------------------------------- 内部工具

def _absorb(st, decision, reason, idfp, cfp, src, rec):
    st.stats[decision] += 1
    st.ring.append({"decision": decision, "reason": reason, "identity_fp": idfp,
                    "content_fp": cfp, "src": src,
                    "preview": redacted_preview(rec)})
    return None


def _is_exact_retransmit(existing, cfp, is_delete) -> bool:
    return (existing["content_fp"] == cfp
            and bool(existing["is_tombstone"]) == is_delete)


def _lower_priority(existing, rank, src) -> bool:
    if rank != existing["src_rank"]:
        return rank > existing["src_rank"]
    # 同优先级确定性决胜：src 字典序靠后的被抑制，避免双源来回翻转
    return src > (existing["src"] or "")


def _is_stale(existing, ts_cmp) -> bool:
    if ts_cmp is None or not existing["event_ts"]:
        return False
    try:
        old = datetime.fromisoformat(existing["event_ts"].replace("Z", "+00:00"))
    except ValueError:
        return False
    return ts_cmp < old


def _edit_reason(kind) -> str:
    return {
        "review": "review_edited",
        "score": "score_refresh",
        "version": "version_revision",
    }.get(kind, "content_changed")


def _store_key(rec, idfp) -> str:
    # 与 keyer.key_of 保持同一不变量；延迟 import 避免循环依赖
    from .keyer import key_of
    return key_of(rec)


def _validate_content(rec) -> None:
    kind = str(rec.get("kind") or "event").strip().lower() or "event"
    for f in _NUMERIC_FIELDS.get(kind, ()):
        if f not in rec or rec[f] is None:
            continue  # 内容字段允许部分缺失
        v = rec[f]
        if isinstance(v, bool) or not _is_number(v):
            raise IdentityError("bad_numeric", {"field": f, "type": type(v).__name__})


def _is_number(v) -> bool:
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        try:
            float(v.strip())
            return True
        except ValueError:
            return False
    return False


def _parse_ts(v):
    """返回 (规范化 ISO 字符串, 可比较 datetime|None)。缺失允许；不可解析 => 不可重试。"""
    if v is None or v == "":
        return "", None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        dt = datetime.fromtimestamp(float(v), tz=timezone.utc)
        return dt.isoformat(), dt
    if not isinstance(v, str):
        raise IdentityError("bad_timestamp", {"type": type(v).__name__})
    s = v.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat(), dt
    except ValueError:
        pass
    if s.isdigit():
        try:
            dt = datetime.fromtimestamp(int(s), tz=timezone.utc)
            return dt.isoformat(), dt
        except (ValueError, OverflowError):
            pass
    raise IdentityError("bad_timestamp", {"type": "str"})


def _quarantine(st: _DedupState, rec, reason: str) -> None:
    digest = hashlib.sha256(
        json.dumps(rec, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()
    kind = str(rec.get("kind") or "").strip().lower()
    _store.quarantine_append(
        st.con, payload_digest=digest, reason=reason,
        field_shape=_store.safe_shape(rec), kind=kind or None, ts=_now_iso())
    _store.audit_append(st.con, identity_fp=digest[:18], content_fp="",
                        decision=QUARANTINED, reason=reason, src=rec.get("src"),
                        rule_version=FP_RULE_VERSION, ts=_now_iso())
    st.stats[QUARANTINED] += 1
    # 诊断仅含摘要与字段形状，不含任何原始值
    st.ring.append({"decision": QUARANTINED, "reason": reason,
                    "payload_digest": digest, "preview": redacted_preview(rec)})


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
