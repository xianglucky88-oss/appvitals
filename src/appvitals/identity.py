"""单一职责：稳定身份与复合指纹（dedup 与 keyer 共用，不含状态、不做决策）。

设计要点（对应验收语义）：
- 身份指纹 identity_fp：只由"记录是什么"的稳定身份元组派生，与可变内容/时间戳/来源无关。
  重传、编辑、删除、回滚只要指向同一身份，identity_fp 恒定 => 存储键恒定，不会产生幽灵行。
- 内容指纹 content_fp：由规范化后的业务内容（剔除来源/时间戳/传输元数据）派生。
  identity_fp 相同而 content_fp 不同 => 同一身份的新状态（编辑/新版本/新评分）。
- 指纹带规则版本前缀（content），升级规则后可识别"按旧规则算的历史行"并重建基线，
  升级本身不制造重复。
- 缺字段走显式 quarantine 语义（IdentityError），不静默用空串凑指纹。
"""
from __future__ import annotations

import hashlib
from typing import Any

# 指纹规则版本：任何会改变 content_fp 计算方式的改动都必须 +1。
# identity 元组结构本身通过 kind 命名空间隔离，结构变更应引入新 kind 而非改旧 kind。
FP_RULE_VERSION = 1

# 来源优先级：数字越小越权威。同优先级按 src 字典序决胜，保证全局确定。
SOURCE_PRIORITY = {
    "official": 1,
    "api": 2,
    "partner": 3,
    "web": 4,
    "rpa": 5,
    "alpha": 3,
    "beta": 4,
}
DEFAULT_SOURCE_PRIORITY = 99

# 原始记录中可能出现的敏感字段：禁止进入任何日志/诊断文本（只允许哈希落库）。
SENSITIVE_KEYS = frozenset({
    "token", "secret", "password", "api_key", "apikey", "authorization",
    "cookie", "email", "phone", "mobile", "id_card", "idcard", "device_id",
    "raw_text", "body",
})


class IdentityError(ValueError):
    """不可重试的身份/字段缺陷。reason 为稳定诊断码，调用方据此 quarantine。"""

    def __init__(self, reason: str, detail: dict[str, Any] | None = None):
        super().__init__(reason)
        self.reason = reason
        # detail 只允许放非敏感的结构化信息（如缺哪个字段、什么类型），不放原始值。
        self.detail = detail or {}


def source_rank(src: Any) -> int:
    s = str(src or "").strip().lower()
    return SOURCE_PRIORITY.get(s, DEFAULT_SOURCE_PRIORITY)


def _clean_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _require(rec: dict, field: str) -> str:
    """身份字段必填且非空白；缺失/空白是不可重试的数据缺陷。"""
    v = _clean_str(rec.get(field))
    if not v:
        raise IdentityError("missing_identity_field", {"field": field})
    return v


def identity_tuple(rec: dict) -> tuple[str, ...]:
    """稳定身份元组。kind 作为命名空间前缀，防止三类记录的 id 互相碰撞。

    - review:  (review, app_id, review_id)
    - version: (version, app_id, version[, build])  build 参与身份 => 同版本不同构建是新记录
    - score:   (score, app_id[, country])
    - event:   (event, id)  兼容历史无类型记录
    """
    kind = _clean_str(rec.get("kind")).lower()
    if kind in ("review", "version", "score"):
        app_id = _require(rec, "app_id")
    if kind == "review":
        return ("review", app_id, _require(rec, "review_id"))
    if kind == "version":
        t = ("version", app_id, _require(rec, "version"))
        build = _clean_str(rec.get("build"))
        return t + ((build,) if build else ())
    if kind == "score":
        t = ("score", app_id)
        country = _clean_str(rec.get("country"))
        return t + ((country.lower(),) if country else ())
    if kind in ("", "event"):
        return ("event", _require(rec, "id"))
    raise IdentityError("unknown_kind", {"kind": kind})


def _hash(parts: list[Any]) -> str:
    blob = _canonical(parts)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _canonical(obj: Any) -> str:
    """确定性 JSON：sort_keys + 统一 None 表示，保证等价内容指纹一致。"""
    import json
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def identity_fp(rec: dict) -> str:
    return "id1:" + _hash(list(identity_tuple(rec)))


# 各 kind 参与内容指纹的业务字段；来源/时间戳/op/传输与身份字段一律不参与。
_CONTENT_FIELDS = {
    "review": ("rating", "title", "text", "lang", "helpful"),
    "version": ("release_notes", "channel", "mandatory"),
    "score": ("rating_avg", "rating_count", "star1", "star2", "star3", "star4", "star5"),
    "event": ("name", "value", "status"),
}


def _content_kind(rec: dict) -> str:
    kind = _clean_str(rec.get("kind")).lower()
    return kind if kind in _CONTENT_FIELDS else "event"


def content_fp(rec: dict) -> str:
    """内容指纹。先过 identity 校验（缺身份字段不允许算内容），再规范化内容字段。"""
    identity_tuple(rec)  # 校验副作用
    kind = _content_kind(rec)
    payload: dict[str, Any] = {}
    for f in _CONTENT_FIELDS[kind]:
        if f not in rec or rec[f] is None:
            continue  # 部分字段缺失 => 仅按可得字段计算，缺失不参与但不报错
        v = rec[f]
        if isinstance(v, str):
            v = v.strip()
        payload[f] = v
    return f"cf{FP_RULE_VERSION}:" + _hash([kind, payload])


def composite_fingerprint(rec: dict) -> tuple[str, str]:
    """稳定复合指纹 (identity_fp, content_fp)。"""
    return identity_fp(rec), content_fp(rec)


def redacted_preview(rec: dict, limit: int = 80) -> str:
    """安全诊断预览：只保留非敏感键的键名与类型/长度，绝不输出原始值。"""
    parts = []
    for k in sorted(rec):
        if k in SENSITIVE_KEYS:
            parts.append(f"{k}=<redacted:{type(rec[k]).__name__}>")
            continue
        v = rec[k]
        if isinstance(v, str):
            parts.append(f"{k}=<str:{len(v)}>")
        else:
            parts.append(f"{k}=<{type(v).__name__}>")
    text = "{" + ",".join(parts) + "}"
    return text[:limit]


def redact_text(text: str) -> str:
    """兜底：把疑似敏感键值对从自由文本里抹掉（防御性，正常路径不拼原始值）。"""
    import re
    return re.sub(
        r"(?i)(token|secret|password|api[_-]?key|authorization|cookie|email|phone)\s*[=:]\s*\S+",
        r"\1=<redacted>",
        str(text),
    )
