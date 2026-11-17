"""单一职责：跨商店评论分页的可恢复采集（review_page）。

输入契约（一条 envelope = 抓取端对一个评论页的回放记录）::

    {
      "kind": "review_page",            # 入口判别标记；缺少此标记的记录原样透传
      "app_id": "app-x", "store": "alpha",
      "page_number": 2,                 # 页码模式：与 cursor 二选一
      "cursor": "c1", "next_cursor": "c2", "anchor": "c0",   # 游标模式
      "has_more": true,
      "reviews": [ {评论事件}, ... ]     # 缺失/空列表即空页
    }

评论事件::

    {"id": "rv-1", "event": "upsert|edit|delete",
     "revision": 3, "ts": "2026-11-03T10:00:00Z",
     "version": "1.2.0", "rating": 5, "author": ..., "title": ..., "content": ...}

输出契约（process 对 envelope 返回记录列表，逐条交给 keyer/store 幂等落库）：

1. 评论实体 ``kind=review``，稳定 id ``rv:{app}:{store}:{review_id}``；
2. 断点 ``kind=review_page_checkpoint``，稳定 id ``rpckpt:{app}:{store}``；
3. 错误记录 ``kind=review_page_error``，按内容哈希生成稳定 id，重放不产生新行。

停止条件（任一成立即不再向后翻页，并写入 checkpoint）：
  * 空页且 has_more=false（end_of_pages，正常收敛）；
  * 命中已消费的页码/游标（pagination_loop，重复批次）；
  * 已结束后再收到页（after_exhaustion）；
  * 页级异常：锚点失效、缺游标/页码、模式切换、payload 非法（blocked，断点不前进，
    修正批次后可从断点重放）。
单条事件异常（缺 id、revision 非法）只产生错误分类计数，不拖垮整页。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional

MARK = "review_page"
STAGE = "_stage_review_page"

# 同 (revision, ts) 下的事件总序：delete > edit > upsert，保证乱序回放收敛一致。
_EVENT_RANK = {"upsert": 0, "edit": 1, "delete": 2}
_VALID_EVENTS = frozenset(_EVENT_RANK)


# --------------------------------------------------------------------------- 状态
@dataclass
class _Cursor:
    mode: str                              # "page_number" | "cursor"
    consumed: set[str] = field(default_factory=set)
    last_token: Optional[str] = None
    last_anchor: Optional[str] = None
    next_position: Any = None
    gap_positions: set[str] = field(default_factory=set)
    saw_end: bool = False                  # 已收到 has_more=false 的末页声明
    exhausted: bool = False
    loop: bool = False


@dataclass
class _Session:
    app_id: str
    store: str
    cur: Optional[_Cursor] = None
    blocked: bool = False
    block_reason: Optional[str] = None
    # rid -> 已物化的评论实体（最新收敛态）
    reviews: dict[str, dict] = field(default_factory=dict)
    # 已应用事件去重键 (rid, event, revision, ts)
    applied: set[tuple] = field(default_factory=set)
    error_ids: set[str] = field(default_factory=set)
    stats: dict[str, int] = field(default_factory=lambda: {
        "pages_seen": 0, "pages_applied": 0, "empty_pages": 0,
        "stale_pages": 0, "out_of_order_pages": 0, "loop_pages": 0,
        "blocked_pages": 0, "events_seen": 0, "events_applied": 0,
        "events_duplicate": 0, "events_invalid": 0, "missing_rating": 0,
    })


# 进程内会话状态；跨进程恢复用 load_state(con) 从 sqlite 断点重建。
_STATE: dict[str, _Session] = {}


def reset_state() -> None:
    """清空进程内会话（测试隔离 / 同一进程内开启新一轮独立采集时使用）。"""
    _STATE.clear()


def _session_key(app_id: str, store: str) -> str:
    return f"{app_id}\x1f{store}"


def _digest(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()[:12]


# --------------------------------------------------------------------------- 入口
def process(record: dict, *, state: Optional[dict[str, _Session]] = None) -> Any:
    """流水线阶段入口。

    非 review_page envelope 原样透传（不增加任何字段），保证相邻链路契约不变；
    envelope 经 process_page 处理后返回待落库记录列表（pipeline 负责摊平）。
    """
    if not isinstance(record, dict) or record.get("kind") != MARK:
        return record
    return process_page(record, state=state)["records"]


def process_page(envelope: dict, *, state: Optional[dict[str, _Session]] = None) -> dict:
    """处理单个分页 envelope，返回 receipt（含 records / status / 分类统计）。"""
    state = _STATE if state is None else state
    app_id = str(envelope.get("app_id") or "").strip()
    store = str(envelope.get("store") or "").strip()

    # --- 信封级非法：不建会话、不前进任何断点，错误记录本身幂等 ---
    if not app_id or not store:
        err = _error_record(app_id or "unknown", store or "unknown",
                            "invalid_envelope", envelope, page=None,
                            detail="app_id/store 缺失")
        return _receipt("blocked", "invalid_envelope", [err], blocked=True)

    sess = state.setdefault(_session_key(app_id, store), _Session(app_id, store))
    sess.stats["pages_seen"] += 1

    mode = _detect_mode(envelope)
    if mode is None:
        return _block(sess, app_id, store, envelope, None,
                      "invalid_pagination", "page_number/cursor 均缺失或非法")
    if sess.cur is not None and sess.cur.mode != mode:
        return _block(sess, app_id, store, envelope, None,
                      "invalid_pagination",
                      f"分页模式切换: {sess.cur.mode} -> {mode}")

    token, position = _position_token(mode, envelope)
    if token is None:
        return _block(sess, app_id, store, envelope, None,
                      "invalid_pagination", "分页位置非法")

    if sess.cur is None:
        sess.cur = _Cursor(mode=mode)
    cur = sess.cur

    if sess.blocked:
        if _is_retry(sess, mode, token):
            # 修正批次重放：页级异常时断点未前进，同一预期位置可解除阻断后重试。
            sess.blocked = False
            sess.block_reason = None
        else:
            return _block(sess, app_id, store, envelope, None,
                          "session_blocked",
                          f"会话已阻断: {sess.block_reason}")

    # --- 停止条件：循环 / 已结束 ---
    if token in cur.consumed:
        sess.stats["loop_pages"] += 1
        cur.loop = True
        return _receipt("stopped", "pagination_loop",
                        _checkpoint(sess), stop_page=token)
    if cur.exhausted:
        if _is_reopen(cur, mode, token):
            # 增量开窗：上一窗口在 has_more=false 收敛后，从下一预期位置继续采集。
            cur.exhausted = False
        else:
            sess.stats["stale_pages"] += 1
            return _receipt("stopped", "after_exhaustion",
                            _checkpoint(sess), stop_page=token)

    # --- 锚点校验：anchor 必须与上一页落点一致；首页锚点只允许为 None/空。
    # 不一致则整页不应用、断点不前进，修正批次可在同位置重放。 ---
    anchor = envelope.get("anchor")
    if anchor is not None and str(anchor).strip():
        if cur.last_anchor is None or str(anchor) != cur.last_anchor:
            return _block(sess, app_id, store, envelope, token, "anchor_invalid",
                          f"anchor={anchor!r} 与断点 {cur.last_anchor!r} 不一致")

    # --- 页码序列：乱序到达的未消费页一律接受，仅记录缺口；重复页在上方判循环。
    # 缺口/乱序状态按已消费集合派生，保证与页到达顺序无关（乱序回放收敛）。 ---
    out_of_order = False
    if mode == "page_number":
        expected = max([n for t, n in _iter_numbers(cur.consumed)], default=0) + 1
        if position > expected:
            out_of_order = True
        for missing in range(1, position):
            tok = f"n:{missing}"
            if tok not in cur.consumed:
                cur.gap_positions.add(tok)
        cur.gap_positions.discard(token)

    raw_events = envelope.get("reviews")
    if raw_events is None:
        raw_events = []
    if not isinstance(raw_events, list):
        return _block(sess, app_id, store, envelope, token,
                      "invalid_payload", "reviews 不是列表")

    # --- 游标模式：has_more 但不给 next_cursor 则无法安全继续 ---
    has_more = bool(envelope.get("has_more", False))
    if mode == "cursor" and has_more and not envelope.get("next_cursor"):
        return _block(sess, app_id, store, envelope, token,
                      "missing_next_cursor", "has_more=true 但 next_cursor 缺失")

    records: list[dict] = []
    applied_now = 0

    if not raw_events:
        sess.stats["empty_pages"] += 1

    # 页内重复 review id：按总序键选胜者，同键再按内容哈希决胜，与输入顺序无关。
    winners = _page_winners(raw_events, sess)
    for ev, page_err in winners:
        if page_err is not None:
            records.append(_error_record(app_id, store, page_err[0], envelope,
                                         page=token, detail=page_err[1]))
            sess.stats["events_invalid"] += 1
            continue
        sess.stats["events_seen"] += 1
        out = _apply_event(sess, app_id, store, ev)
        if out is None:
            # 重复/旧事件计数已在 _apply_event 内完成，这里跳过即可。
            continue
        sess.stats["events_applied"] += 1
        applied_now += 1
        if out.pop("_rating_missing", False):
            sess.stats["missing_rating"] += 1
        records.append(out)

    # --- 前进断点（仅在页面被接受之后） ---
    cur.consumed.add(token)
    cur.last_token = token
    # 锚点对齐上游约定：页码用页码本身，游标用原始 cursor 字符串（envelope.anchor 同形）。
    cur.last_anchor = position
    if mode == "page_number":
        cur.next_position = position + 1
        # 乱序安全的耗尽判定：只要已见过末页声明且 1..N 连续无缺口即结束，
        # 与末页到达的先后顺序无关（补齐缺口后自动重新收敛为 exhausted）。
        numbers = sorted(n for _t, n in _iter_numbers(cur.consumed))
        contiguous = numbers == list(range(1, len(numbers) + 1))
        if not has_more:
            cur.saw_end = True
        cur.exhausted = bool(cur.saw_end and contiguous)
    else:
        cur.next_position = envelope.get("next_cursor") if has_more else None
        if not has_more:
            cur.saw_end = True
            cur.exhausted = True
    if out_of_order:
        sess.stats["out_of_order_pages"] += 1
    sess.stats["pages_applied"] += 1

    records.append(_checkpoint(sess))
    status = "ok"
    stop_reason = None
    if cur.exhausted:
        status, stop_reason = "stopped", "end_of_pages"
    return _receipt(status, stop_reason, records,
                    stop_page=token, applied=applied_now,
                    out_of_order=out_of_order)


# --------------------------------------------------------------------------- 事件处理
def _page_winners(raw_events: list, sess: _Session):
    """页内按 review id 去重，返回 [(event, None) | (None, (code, detail))]，保持原序。"""
    picked: dict[str, tuple[tuple, int, dict]] = {}
    order: list[str] = []
    invalid: list[tuple[None, tuple]] = []
    for ev in raw_events:
        if not isinstance(ev, dict) or not str(ev.get("id") or "").strip():
            invalid.append((None, ("invalid_event", "事件缺 id 或非对象")))
            continue
        kind = str(ev.get("event") or "").strip().lower()
        if kind not in _VALID_EVENTS:
            invalid.append((None, ("invalid_event", f"event 非法: {ev.get('event')!r}")))
            continue
        revision = _revision(ev)
        if revision is None:
            invalid.append((None, ("invalid_event", "revision 非整数")))
            continue
        rid = str(ev["id"]).strip()
        if rid not in picked:
            order.append(rid)
        key = (revision, str(ev.get("ts") or ""), _EVENT_RANK[kind])
        tie = _digest(ev)
        if rid not in picked or (key, tie) > (picked[rid][0], picked[rid][1]):
            picked[rid] = (key, tie, ev)
    return [(picked[rid][2], None) for rid in order] + invalid


def _revision(ev: dict) -> Optional[int]:
    rev = ev.get("revision", 1)
    try:
        return int(rev)
    except (TypeError, ValueError):
        return None


def _apply_event(sess: _Session, app_id: str, store: str, ev: dict) -> Optional[dict]:
    rid = str(ev["id"]).strip()
    kind = str(ev["event"]).strip().lower()
    revision = _revision(ev)
    ts = str(ev.get("ts") or "")
    dedup_key = (rid, kind, revision, ts)
    if dedup_key in sess.applied:
        sess.stats["events_duplicate"] += 1
        return None

    current = sess.reviews.get(rid)
    order_key = (revision, ts, _EVENT_RANK[kind])
    if current is not None and order_key <= tuple(current["_last_key"]):
        # 旧事件迟到：不允许覆盖较新状态（重复批次/乱序收敛的核心保证）。
        sess.stats["events_duplicate"] += 1
        return None
    sess.applied.add(dedup_key)

    rating_present = "rating" in ev and ev.get("rating") is not None
    if kind == "upsert":
        review = _base_review(app_id, store, rid, ev)
        review["revision"] = revision
        review["event_ts"] = ts
        review["status"] = "active"
        review["rating"] = ev.get("rating") if rating_present else None
        review["rating_missing"] = not rating_present
        review["version"] = ev.get("version")
        review["updated_at"] = ts
    elif kind == "edit":
        if current is None:
            # 编辑先于首录到达：补建占位实体，后续 upsert 按总序键决定是否覆盖。
            review = _base_review(app_id, store, rid, ev)
            review["status"] = "active"
            review["rating"] = None
            review["rating_missing"] = True
            review["version"] = None
        else:
            review = dict(current)
            review.pop("_last_key", None)
            review["history"] = list(current.get("history", []))
        for f in ("author", "title", "content"):
            if f in ev:
                review[f] = ev[f]
        # 编辑缺评分/版本：保留上一版值，不让时间线出现空洞。
        if rating_present:
            review["rating"] = ev.get("rating")
            review["rating_missing"] = False
        if ev.get("version") is not None:
            review["version"] = ev.get("version")
        review["revision"] = revision
        review["event_ts"] = ts
        review["updated_at"] = ts
    else:  # delete
        if current is None:
            review = _base_review(app_id, store, rid, ev)
            review["rating"] = None
            review["rating_missing"] = True
            review["version"] = ev.get("version")
        else:
            review = dict(current)
            review.pop("_last_key", None)
            review["history"] = list(current.get("history", []))
        review["status"] = "deleted"
        review["revision"] = revision
        review["event_ts"] = ts
        review["updated_at"] = ts

    review["history"].append({"event": kind, "revision": revision,
                              "ts": ts, "version": review.get("version")})
    review["_last_key"] = list(order_key)
    sess.reviews[rid] = review

    out = dict(review)
    out.pop("_last_key", None)
    out["_rating_missing"] = bool(review["rating_missing"])
    out[STAGE] = "ok"
    return out


def _base_review(app_id: str, store: str, rid: str, ev: dict) -> dict:
    return {
        "id": f"rv:{app_id}:{store}:{rid}",
        "kind": "review",
        "app_id": app_id,
        "store": store,
        "review_id": rid,
        "event": str(ev.get("event") or "").strip().lower(),
        "author": ev.get("author"),
        "title": ev.get("title"),
        "content": ev.get("content"),
        "payload": ev,
        "history": [],
    }


# --------------------------------------------------------------------------- 断点/错误
def _checkpoint(sess: _Session) -> dict:
    cur = sess.cur
    return {
        "id": f"rpckpt:{sess.app_id}:{sess.store}",
        "kind": "review_page_checkpoint",
        "app_id": sess.app_id,
        "store": sess.store,
        "mode": cur.mode if cur else None,
        "consumed": sorted(cur.consumed) if cur else [],
        "gap_positions": sorted(cur.gap_positions) if cur else [],
        "next_position": cur.next_position if cur else None,
        "last_token": cur.last_token if cur else None,
        "last_anchor": cur.last_anchor if cur else None,
        "saw_end": cur.saw_end if cur else False,
        "exhausted": bool(cur and cur.exhausted),
        "loop": bool(cur and cur.loop),
        "blocked": sess.blocked,
        "block_reason": sess.block_reason,
        "stats": dict(sess.stats),
        STAGE: "error" if sess.blocked else "ok",
    }


def _error_record(app_id: str, store: str, code: str, envelope: Any,
                  page: Optional[str], detail: str) -> dict:
    eid = f"rperr:{app_id}:{store}:{code}:{_digest({'p': page, 'e': envelope})}"
    return {
        "id": eid,
        "kind": "review_page_error",
        "app_id": app_id,
        "store": store,
        "code": code,
        "severity": "error",
        "permanent": code in ("invalid_envelope", "invalid_pagination",
                              "invalid_payload", "invalid_event"),
        "page": page,
        "detail": detail,
        STAGE: "error",
    }


def _block(sess: Optional[_Session], app_id: str, store: str, envelope: dict,
           page: Optional[str], code: str, detail: str) -> dict:
    records = [_error_record(app_id, store, code, envelope, page, detail)]
    if sess is not None:
        sess.blocked = True
        sess.block_reason = code
        sess.stats["blocked_pages"] += 1
        records.append(_checkpoint(sess))
    return _receipt("blocked", code, records, stop_page=page, blocked=True)


def _receipt(status: str, stop_reason: Optional[str], records,
             stop_page: Optional[str] = None, applied: int = 0,
             blocked: bool = False, out_of_order: bool = False) -> dict:
    if isinstance(records, dict):
        records = [records]
    return {"status": status, "stop_reason": stop_reason,
            "stop_page": stop_page, "applied": applied,
            "blocked": blocked, "out_of_order": out_of_order,
            "records": records}


# --------------------------------------------------------------------------- 辅助
def _detect_mode(envelope: dict) -> Optional[str]:
    has_pn = envelope.get("page_number") is not None
    has_cur = envelope.get("cursor") is not None
    if has_pn == has_cur:
        return None
    if has_pn:
        try:
            int(envelope["page_number"])
        except (TypeError, ValueError):
            return None
        return "page_number"
    return "cursor"


def _position_token(mode: str, envelope: dict):
    if mode == "page_number":
        try:
            n = int(envelope["page_number"])
        except (TypeError, ValueError):
            return None, None
        return f"n:{n}", n
    cur = str(envelope["cursor"])
    return f"c:{cur}", cur


def _iter_numbers(tokens: set[str]):
    for t in tokens:
        if t.startswith("n:"):
            yield t, int(t[2:])


def _is_retry(sess: _Session, mode: str, token: str) -> bool:
    """阻断恢复判定：同一模式、同一未消费的预期位置上的修正页才允许重试。"""
    if sess.cur is None or sess.cur.mode != mode or token in sess.cur.consumed:
        return False
    if mode == "page_number":
        expected = max([n for _t, n in _iter_numbers(sess.cur.consumed)],
                       default=0) + 1
        return token == f"n:{expected}"
    return True   # 游标模式：新游标即修正批次的重放页


def _is_reopen(cur: _Cursor, mode: str, token: str) -> bool:
    """耗尽后增量开窗判定：页码必须严格接续；游标为未消费的新游标。"""
    if mode == "page_number":
        next_n = max([n for _t, n in _iter_numbers(cur.consumed)], default=0) + 1
        return token == f"n:{next_n}"
    return True


# --------------------------------------------------------------------------- 跨进程恢复
def load_state(con) -> dict[str, _Session]:
    """从 sqlite 已落库记录重建会话状态，使新一轮 process 从断点继续（中途恢复）。"""
    from . import store as _store
    state: dict[str, _Session] = {}
    for _key, data in _store.all_records(con):
        kind = data.get("kind")
        if kind == "review_page_checkpoint":
            sk = _session_key(data["app_id"], data["store"])
            sess = state.setdefault(sk, _Session(data["app_id"], data["store"]))
            cur = _Cursor(mode=data["mode"], saw_end=data.get("saw_end", False),
                          exhausted=data.get("exhausted", False),
                          loop=data.get("loop", False))
            cur.consumed = set(data.get("consumed", []))
            cur.gap_positions = set(data.get("gap_positions", []))
            cur.last_token = data.get("last_token")
            if cur.last_token:
                # 与 process_page 的 last_anchor 同形：页码存 int，游标存原始字符串。
                cur.last_anchor = int(cur.last_token[2:]) if cur.mode == "page_number" \
                    else cur.last_token[2:]
            cur.next_position = data.get("next_position")
            sess.cur = cur
            sess.blocked = data.get("blocked", False)
            sess.block_reason = data.get("block_reason")
            sess.stats.update(data.get("stats", {}))
        elif kind == "review":
            sk = _session_key(data["app_id"], data["store"])
            sess = state.setdefault(sk, _Session(data["app_id"], data["store"]))
            rid = data["review_id"]
            sess.reviews[rid] = dict(data)
            # 收敛序键取历史最后一条事件（delete/edit 后当前 event 字段未必等于末条）。
            history = data.get("history", [])
            last = history[-1] if history else {
                "event": data.get("event", "upsert"),
                "revision": data.get("revision", 1),
                "ts": data.get("event_ts", ""),
            }
            sess.reviews[rid]["_last_key"] = [
                last["revision"], last.get("ts", ""),
                _EVENT_RANK.get(last.get("event", "upsert"), 0),
            ]
            for h in history:
                sess.applied.add((rid, h["event"], h["revision"], h.get("ts", "")))
        elif kind == "review_page_error":
            sk = _session_key(data["app_id"], data["store"])
            sess = state.setdefault(sk, _Session(data["app_id"], data["store"]))
            sess.error_ids.add(data["id"])
    _STATE.clear()
    _STATE.update(state)
    return state
