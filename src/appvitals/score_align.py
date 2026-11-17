"""单一职责：把版本发布时间、评分口径、评论数量与星级分布对齐到同一条时间轴。

对外接口与基线保持一致：``process(record: Mapping) -> dict``。输入记录的原有字段
全部原样保留（含调用方已有的 ``_stage_score_align``）；派生结果只写入新增的
``_score_align`` 诊断字段，``_stage_score_align`` 仅在缺失时按基线补 ``"naive"``，
不覆盖已有值。因此：

* 不含评分/评论/版本信号的旧输入 → 原始字段逐字段不变，``_score_align.status`` 为
  ``passthrough``；
* ``process`` 是纯函数式变换，不修改入参，重复调用/并发调用结果一致；
* 单个畸形字段只降级为诊断告警，不打断流水线。

本模块自己的数据不变量
-----------------------
1. **统一时间轴**：所有时间点解析为带时区的 ``datetime`` 再换算成 UTC；输出同时保留
   原始本地偏移（``local`` / ``offset_seconds``），让"同一时刻"可解释。裸时间按记录
   声明的时区解释（缺省 UTC），并逐条标记 ``assumed_timezone``。
2. **统一评分口径**：跨店铺评分按 ``rating_scale`` / ``rating_unit`` 归一化到 0..5 星；
   ``percent`` 按 100 分制、``points`` 按声明分母、``"a/b"`` 按自身分母换算。
3. **半星精度**：归一到最近的 0.5 星（``ROUND_HALF_UP``），用 0..10 的整数
   ``half_steps`` 表达以避免浮点误差；原始值与原始分母保留在 ``raw`` / ``raw_scale``。
4. **评论数量可核对**：``reported``（输入声明）与 ``observed``（去重后可观测）分开；
   重复评论按 id（无 id 时按内容哈希）去重，重复导入不会放大数量或分布。
5. **无评论区间**：只有输入能证明"没有评论"时才推导（显式零评论窗口，或声明评论事件
   完整且给出覆盖区间）；稀疏评论本身不臆造零值。
6. **迟到评论**：显式 ``late`` 标记、``received_at`` 滞后超过声明阈值
   （``late_threshold_seconds``；未声明时不做滞后判定）、或晚于观测时刻的评论计入
   ``late``，但仍参与评论数量、星级分布与时间轴，不丢弃。
7. **重复运行/重入**：派生值只依赖记录中的原始信号，派生字段不会被再次读取，
   所以 ``process(process(r)) == process(r)``；``process`` 无模块级可变状态，
   并发调用互不影响。
8. **失败恢复**：畸形评分/时间/评论只产生告警并降级；单条坏评论跳过不拖累其他评论；
   内部异常回退为 ``status=recovered``；非映射入参属于接口错误，抛 ``TypeError``。

诊断字段 ``_score_align.schema`` 固定为 ``score_align.v1``；``warnings`` 按
``(code, detail)`` 排序并去重，便于回归比对。``timeline.events`` 是全量时间轴的
有界投影：评论超过 ``MAX_EVENTS`` 时保留最早的若干条（``events_truncated=true``），
版本发布与观测两个锚点始终保留。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - 运行栈为 3.14
    ZoneInfo = None  # type: ignore[assignment]

SCHEMA = "score_align.v1"
CANONICAL_MAX = Decimal("5")
HALF_STEP = Decimal("0.5")
UTC = timezone.utc
MAX_EVENTS = 2000

# 输入字段名（按优先级）。刻意不读取 ``value``：它是通用字段而非评分。
_RATING_KEYS = ("rating", "average_rating", "avg_rating", "score", "stars", "rating_value")
_RATING_SCALE_KEYS = ("rating_scale", "rating_max", "score_scale", "score_max", "rating_range")
_RATING_UNIT_KEYS = ("rating_unit", "score_unit", "rating_type", "score_type")
_RATING_DIST_KEYS = ("rating_distribution", "rating_histogram", "star_distribution",
                     "score_distribution", "histogram")
_REVIEW_COUNT_KEYS = ("review_count", "reviews_count", "rating_count", "ratings_count",
                      "num_reviews", "total_reviews")
_REVIEW_KEYS = ("reviews", "review_items", "review_list")
_REVIEW_WINDOW_KEYS = ("review_windows", "review_periods", "review_buckets")
_RELEASE_KEYS = ("released_at", "release_at", "release_ts", "published_at",
                 "version_released_at", "version_published_at", "release_date")
_OBSERVED_KEYS = ("observed_at", "snapshot_at", "as_of", "fetched_at", "captured_at",
                  "collected_at")
_TIMEZONE_KEYS = ("timezone", "tz", "store_timezone", "default_timezone", "store_tz")
_LATE_THRESHOLD_KEYS = ("late_threshold_seconds", "late_after_seconds", "stale_after_seconds")
_COMPLETENESS_KEYS = ("review_completeness", "review_events_complete", "reviews_complete")

_REVIEW_TS_KEYS = ("ts", "timestamp", "created_at", "published_at", "date", "time")
_REVIEW_RECEIVED_KEYS = ("received_at", "ingested_at", "collected_at", "arrived_at")
_REVIEW_RATING_KEYS = ("rating", "score", "stars")
_REVIEW_ID_KEYS = ("id", "review_id", "uid")

_PERCENT_UNITS = {"percent", "percentage", "pct", "%"}
_POINT_UNITS = {"point", "points", "point10", "ten_point"}
_COMPLETE_VALUES = {"complete", "full", "exhaustive", "all"}


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
def _first(scope: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    """按优先级取第一个非空值；空白字符串视为缺失，避免遮蔽后面的别名。"""
    for key in keys:
        if key in scope:
            value = scope[key]
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return None


def _has_any(scope: Mapping[str, Any], keys: tuple[str, ...]) -> bool:
    return any(key in scope and scope[key] is not None for key in keys)


def _warn(warnings: list[dict[str, str]], code: str, detail: str = "") -> None:
    item = {"code": code, "detail": detail}
    if item not in warnings:
        warnings.append(item)


def _sorted_warnings(warnings: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(warnings, key=lambda item: (item["code"], item["detail"]))


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return repr(value)


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=_json_default)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _as_decimal(value: Any) -> Decimal | None:
    """把数字/数字字符串（允许千分位、百分号、尾部空白）转成 Decimal。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
                return None
            return Decimal(str(value))
        except InvalidOperation:  # pragma: no cover - 防御
            return None
    if isinstance(value, str):
        text = value.strip().replace("_", "")
        if not text:
            return None
        if text.endswith("%"):
            text = text[:-1].strip()
        # 千分位用逗号（1,234）；小数逗号只出现在单逗号且尾随 1-2 位时。
        if re.fullmatch(r"[+-]?\d{1,3},\d{3}(,\d{3})*", text):
            text = text.replace(",", "")
        elif re.fullmatch(r"[+-]?\d+,\d{1,2}", text):
            text = text.replace(",", ".")
        try:
            return Decimal(text)
        except InvalidOperation:
            return None
    return None


def _as_int(value: Any) -> int | None:
    number = _as_decimal(value)
    if number is None or number != number.to_integral_value():
        return None
    return int(number)


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value == 1:
        return True
    return isinstance(value, str) and value.strip().lower() in {"true", "yes", "1", "late"}


def _is_blank(value: Any) -> bool:
    return isinstance(value, str) and not value.strip()


def _iso_utc(value: datetime) -> str:
    text = value.astimezone(UTC).isoformat()
    return text[:-6] + "Z" if text.endswith("+00:00") else text


def _iso_local(value: datetime) -> str:
    return value.isoformat()


def _format_offset(offset: timedelta) -> str:
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hours, minutes = divmod(total // 60, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def _parse_offset(text: str) -> timezone | None:
    """解析 ``Z`` / ``UTC`` / ``UTC+8`` / ``GMT-05:30`` / ``+08:00`` / ``+0800``。"""
    token = text.strip()
    if token.upper() in {"Z", "UTC", "GMT"}:
        return UTC
    prefixed = re.fullmatch(r"(?:UTC|GMT)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?",
                            token, re.IGNORECASE)
    if prefixed:
        sign = 1 if prefixed.group(1) == "+" else -1
        delta = timedelta(hours=int(prefixed.group(2)), minutes=int(prefixed.group(3) or 0))
        return timezone(sign * delta)
    match = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", token)
    if not match:
        return None
    sign = 1 if match.group(1) == "+" else -1
    delta = timedelta(hours=int(match.group(2)), minutes=int(match.group(3)))
    return timezone(sign * delta)


def _resolve_timezone(record: Mapping[str, Any], warnings: list[dict[str, str]]) -> tuple[timezone, str, bool]:
    """返回 (tzinfo, 展示名, 是否缺省/回退)。"""
    spec = _first(record, _TIMEZONE_KEYS)
    if spec is None or _is_blank(spec):
        return UTC, "UTC", True
    if isinstance(spec, (int, float)) and not isinstance(spec, bool):
        delta = _numeric_offset(Decimal(str(spec)))
        return timezone(delta), _format_offset(delta), False
    text = str(spec).strip()
    offset = _parse_offset(text)
    if offset is not None:
        return offset, _format_offset(offset.utcoffset(None) or timedelta(0)), False
    if re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
        delta = _numeric_offset(Decimal(text))
        return timezone(delta), _format_offset(delta), False
    if ZoneInfo is not None:
        try:
            return ZoneInfo(text), text, False
        except Exception:
            pass
    _warn(warnings, "unknown_timezone", text)
    return UTC, "UTC", True


def _numeric_offset(amount: Decimal) -> timedelta:
    """数值偏移：绝对值 <= 14 视为小时（Python/JS 常见），否则视为分钟（JS getTimezoneOffset）。

    不带符号的 HHMM 整数与分钟数存在歧义（如 300 既可能是 -05:00 也可能是 03:00），
    因此这里不做 HHMM 猜测；需要 HHMM 时请用 ``"+0800"`` 这类字符串。
    """
    if abs(amount) <= 14:
        return timedelta(hours=float(amount))
    return timedelta(minutes=float(amount))


def _from_epoch(number: float) -> datetime:
    if abs(number) >= 1e14:      # 微秒
        return datetime.fromtimestamp(number / 1e6, tz=UTC)
    if abs(number) >= 1e11:      # 毫秒
        return datetime.fromtimestamp(number / 1e3, tz=UTC)
    return datetime.fromtimestamp(number, tz=UTC)  # 秒


def _parse_time(value: Any, default_tz: timezone, warnings: list[dict[str, str]],
                field: str) -> dict[str, Any] | None:
    """把 ISO 字符串/epoch/date 归一成带 UTC 与本地偏移的解析结果。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    assumed = False
    source = str(value)
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=default_tz)
            assumed = True
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            dt = _from_epoch(float(value))
        else:
            text = str(value).strip()
            if re.fullmatch(r"\d{8}", text):          # YYYYMMDD
                dt = datetime.strptime(text, "%Y%m%d").replace(tzinfo=default_tz)
                assumed = True
            elif re.fullmatch(r"\d{14}", text):       # YYYYMMDDHHMMSS
                dt = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=default_tz)
                assumed = True
            elif re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
                dt = _from_epoch(float(text))
            else:
                text = text.replace("/", "-")
                if re.match(r"^\d{4}-\d{2}-\d{2} ", text):
                    text = text.replace(" ", "T", 1)
                text = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", text)
                if text.endswith(("Z", "z")):
                    text = text[:-1] + "+00:00"
                dt = datetime.fromisoformat(text)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=default_tz)
                    assumed = True
    except (ValueError, TypeError, OverflowError, OSError) as exc:
        _warn(warnings, "invalid_timestamp", f"{field}:{source}:{type(exc).__name__}")
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
        assumed = True
    offset = dt.utcoffset() or timedelta(0)
    utc_dt = dt.astimezone(UTC)
    return {
        "utc": _iso_utc(utc_dt),
        "local": _iso_local(dt),
        "offset_seconds": int(offset.total_seconds()),
        "source": source,
        "assumed_timezone": assumed,
        "_dt": utc_dt,
    }


def _public_time(parsed: dict[str, Any] | None) -> dict[str, Any] | None:
    if parsed is None:
        return None
    return {key: value for key, value in parsed.items() if key != "_dt"}


# --------------------------------------------------------------------------- #
# 评分口径
# --------------------------------------------------------------------------- #
def _resolve_unit(scope: Mapping[str, Any], raw_value: Any = None) -> str:
    unit = _first(scope, _RATING_UNIT_KEYS)
    text = str(unit).strip().lower() if unit is not None else ""
    if text in _PERCENT_UNITS or "%" in text or "percent" in text:
        return "percent"
    if "star" in text:
        return "stars"
    if text in _POINT_UNITS or "point" in text or "ten" in text:
        return "points"
    if isinstance(raw_value, str) and raw_value.strip().endswith("%"):
        return "percent"
    if text:
        return "points"
    # 未声明单位时看分母：5 分制视为星级，其余视为分制。
    declared = _first(scope, _RATING_SCALE_KEYS)
    number = _as_decimal(declared[-1] if isinstance(declared, (list, tuple)) and declared else declared)
    if number is not None and number != CANONICAL_MAX:
        return "points"
    return "stars"


_RATING_OBJECT_SCALE_KEYS = ("scale", "max", "range", "max_value", "out_of")
_RATING_OBJECT_UNIT_KEYS = ("unit", "type", "kind", "format")


def _rating_parts(raw_value: Any) -> tuple[Any, Mapping[str, Any] | None]:
    """评分既可能是标量，也可能是 ``{"value": 8.7, "scale": 10}`` 形式的对象。"""
    if not isinstance(raw_value, Mapping):
        return raw_value, None
    value = raw_value.get("value", raw_value.get("score", raw_value.get("rating")))
    inline: dict[str, Any] = dict(raw_value)
    for alias in _RATING_OBJECT_SCALE_KEYS:
        if alias in raw_value and not any(k in raw_value for k in _RATING_SCALE_KEYS):
            inline["rating_scale"] = raw_value[alias]
            break
    for alias in _RATING_OBJECT_UNIT_KEYS:
        if alias in raw_value and not any(k in raw_value for k in _RATING_UNIT_KEYS):
            inline["rating_unit"] = raw_value[alias]
            break
    return value, inline


def _resolve_scale(scope: Mapping[str, Any], unit: str, warnings: list[dict[str, str]]) -> Decimal:
    raw = _first(scope, _RATING_SCALE_KEYS)
    if raw is None:
        if unit == "percent":
            return Decimal("100")
        if unit == "points":
            _warn(warnings, "rating_scale_defaulted", "points=10")
            return Decimal("10")
        return CANONICAL_MAX  # 星级的自然默认，不必告警
    if isinstance(raw, (list, tuple)):
        raw = raw[-1] if raw else None
    if isinstance(raw, str) and re.search(r"\d\s*[-/\.]\s*\d", raw):
        raw = re.split(r"[-/\.]", raw)[-1]
    number = _as_decimal(raw)
    if number is None or number <= 0:
        _warn(warnings, "invalid_rating_scale", str(raw))
        return Decimal("100") if unit == "percent" else CANONICAL_MAX
    return number


def _merge_rating_scope(base: Mapping[str, Any],
                        override: Mapping[str, Any]) -> Mapping[str, Any]:
    """把条目/嵌套对象声明的口径叠加到记录级口径上。

    口径是一个整体（单位与分母要配套），因此 override 只声明其一时不继承 base 的
    另一项：只给分母 → 单位按分母重新推断；只给单位 → 分母按单位取默认值。
    """
    declared_scale = _has_any(override, _RATING_SCALE_KEYS)
    declared_unit = _has_any(override, _RATING_UNIT_KEYS)
    if not declared_scale and not declared_unit:
        return base
    merged = dict(base)
    if declared_scale:
        for key in _RATING_SCALE_KEYS:
            if override.get(key) is not None:
                merged[key] = override[key]
        if not declared_unit:
            for key in _RATING_UNIT_KEYS:
                merged.pop(key, None)
    if declared_unit:
        for key in _RATING_UNIT_KEYS:
            if override.get(key) is not None:
                merged[key] = override[key]
        if not declared_scale:
            for key in _RATING_SCALE_KEYS:
                merged.pop(key, None)
    return merged


def _normalize_rating(raw_value: Any, scope: Mapping[str, Any], warnings: list[dict[str, str]],
                      prefix: str = "") -> dict[str, Any] | None:
    raw_value, inline_scope = _rating_parts(raw_value)
    if inline_scope is not None:
        # 对象自带的 scale/unit 优先于记录级口径。
        scope = _merge_rating_scope(scope, inline_scope)

    # "4/5" 形式：分母即本条评分自己的 scale。
    inline_scale: Any = None
    if isinstance(raw_value, str) and re.fullmatch(r"\s*\d+(?:[.,]\d+)?\s*/\s*\d+(?:[.,]\d+)?\s*",
                                                  raw_value):
        numerator, denominator = raw_value.split("/", 1)
        raw_value, inline_scale = numerator.strip(), denominator.strip()

    raw = _as_decimal(raw_value)
    if raw is None:
        if raw_value is not None and not _is_blank(raw_value):
            _warn(warnings, f"invalid_{prefix}rating", str(raw_value))
        return None

    unit = _resolve_unit(scope, raw_value)
    if inline_scale is not None:
        unit = "ratio"
        scale = _as_decimal(inline_scale)
        if scale is None or scale <= 0:
            _warn(warnings, f"invalid_{prefix}rating_scale", str(inline_scale))
            return None
    else:
        scale = _resolve_scale(scope, unit, warnings)

    if unit == "percent":
        stars_exact = raw * CANONICAL_MAX / Decimal("100")
    else:
        stars_exact = raw * CANONICAL_MAX / scale
    if stars_exact < 0 or stars_exact > CANONICAL_MAX:
        clamped = min(CANONICAL_MAX, max(Decimal("0"), stars_exact))
        _warn(warnings, f"{prefix}rating_out_of_range", f"{raw} -> {clamped}")
        stars_exact = clamped

    steps = int((stars_exact / HALF_STEP).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    steps = min(10, max(0, steps))
    stars = Decimal(steps) * HALF_STEP
    return {
        "raw": float(raw) if raw != raw.to_integral_value() else int(raw),
        "raw_scale": float(scale) if scale != scale.to_integral_value() else int(scale),
        "unit": unit,
        "stars": float(stars),
        "half_steps": steps,
        "quantized": stars != stars_exact,
    }


def _scope(entry: Mapping[str, Any], record: Mapping[str, Any]) -> Mapping[str, Any]:
    """条目自带口径字段时以条目为准，缺失字段回退到记录级口径。"""
    return _merge_rating_scope(record, entry)


# --------------------------------------------------------------------------- #
# 评论 / 窗口 / 直方图
# --------------------------------------------------------------------------- #
def _review_identity(entry: Mapping[str, Any], parsed_ts: dict[str, Any] | None,
                     normalized_rating: dict[str, Any] | None) -> str:
    for key in _REVIEW_ID_KEYS:
        if entry.get(key) is not None:
            return f"id:{entry[key]}"
    if entry.get("_container_key") is not None:
        return f"id:{entry['_container_key']}"
    payload = {
        "ts": parsed_ts["_dt"].isoformat() if parsed_ts else None,
        "rating": normalized_rating["raw"] if normalized_rating else None,
        "scale": normalized_rating["raw_scale"] if normalized_rating else None,
        "unit": normalized_rating["unit"] if normalized_rating else None,
        "body": entry.get("body") or entry.get("text") or entry.get("content"),
        "author": entry.get("author") or entry.get("user"),
    }
    return f"hash:{_stable_hash(payload)}"


def _late_reasons(entry: Mapping[str, Any], parsed_ts: dict[str, Any] | None,
                  received: dict[str, Any] | None, threshold: Decimal | None,
                  observed_dt: datetime | None) -> list[str]:
    reasons: list[str] = []
    if _is_true(entry.get("late")) or _is_true(entry.get("is_late")):
        reasons.append("flagged")
    if parsed_ts is not None and received is not None and threshold is not None:
        lag = (received["_dt"] - parsed_ts["_dt"]).total_seconds()
        if lag > float(threshold):
            reasons.append("received_late")
    if parsed_ts is not None and observed_dt is not None and parsed_ts["_dt"] > observed_dt:
        reasons.append("after_observation")
    return reasons


def _review_entries(raw_items: Any, warnings: list[dict[str, str]]) -> list[Any] | None:
    """评论容器既可以是列表，也可以是 ``{review_id: {...}}`` 或 ``{"items": [...]}``。"""
    if isinstance(raw_items, (list, tuple)):
        return list(raw_items)
    if isinstance(raw_items, Mapping):
        inner = _first(raw_items, ("items", "reviews", "data", "list"))
        if isinstance(inner, (list, tuple)):
            return list(inner)
        entries: list[Any] = []
        for key, value in raw_items.items():
            if isinstance(value, Mapping):
                entries.append({"_container_key": key, **value})
            else:
                _warn(warnings, "invalid_review_entry", f"key:{key}:{type(value).__name__}")
        return entries
    if raw_items is not None:
        _warn(warnings, "invalid_reviews_container", type(raw_items).__name__)
    return None


def _collect_reviews(record: Mapping[str, Any], default_tz: timezone, warnings: list[dict[str, str]],
                     threshold: Decimal | None, observed_dt: datetime | None) -> dict[str, Any]:
    raw_items = _first(record, _REVIEW_KEYS)
    entries = _review_entries(raw_items, warnings)
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicates = 0

    for index, entry in enumerate(entries or ()):
        if not isinstance(entry, Mapping):
            _warn(warnings, "invalid_review_entry", f"index:{index}:{type(entry).__name__}")
            continue
        try:
            parsed_ts = _parse_time(_first(entry, _REVIEW_TS_KEYS), default_tz, warnings, "review.ts")
            if parsed_ts is None:
                _warn(warnings, "review_without_timestamp", f"index:{index}")
            received = _parse_time(_first(entry, _REVIEW_RECEIVED_KEYS), default_tz, warnings,
                                   "review.received_at")
            rating = _normalize_rating(_first(entry, _REVIEW_RATING_KEYS), _scope(entry, record),
                                       warnings, "review_")
            identity = _review_identity(entry, parsed_ts, rating)
            if identity in seen:
                duplicates += 1
                continue
            seen.add(identity)
            entry_id = _first(entry, _REVIEW_ID_KEYS)
            if entry_id is None and entry.get("_container_key") is not None:
                entry_id = entry["_container_key"]
            items.append({
                "id": entry_id,
                "ts": parsed_ts,
                "received": received,
                "rating": rating,
                "identity": identity,
                "late_reasons": _late_reasons(entry, parsed_ts, received, threshold, observed_dt),
            })
        except Exception as exc:  # 失败恢复：坏条目跳过并告警。
            _warn(warnings, "review_entry_error", f"index:{index}:{type(exc).__name__}")

    placed = sorted(
        (item for item in items if item["ts"] is not None),
        key=lambda item: (item["ts"]["_dt"], item["identity"]),
    )
    return {
        "present": entries is not None,
        "items": items,
        "placed": placed,
        "duplicates": duplicates,
        "late_items": [item for item in items if item["late_reasons"]],
    }


def _collect_windows(record: Mapping[str, Any], default_tz: timezone,
                     warnings: list[dict[str, str]]) -> list[dict[str, Any]]:
    raw_windows = _first(record, _REVIEW_WINDOW_KEYS)
    if raw_windows is None:
        return []
    if not isinstance(raw_windows, (list, tuple)):
        _warn(warnings, "invalid_review_windows", type(raw_windows).__name__)
        return []
    windows: list[dict[str, Any]] = []
    for index, entry in enumerate(raw_windows):
        if not isinstance(entry, Mapping):
            _warn(warnings, "invalid_review_window", f"index:{index}:{type(entry).__name__}")
            continue
        start = _parse_time(_first(entry, ("start", "from")), default_tz, warnings, "window.start")
        end = _parse_time(_first(entry, ("end", "to")), default_tz, warnings, "window.end")
        if start is None or end is None:
            _warn(warnings, "review_window_without_bounds", f"index:{index}")
            continue
        if end["_dt"] < start["_dt"]:
            start, end = end, start
            _warn(warnings, "review_window_reversed", f"index:{index}")
        count = _as_int(_first(entry, ("count", "review_count")))
        if _has_any(entry, ("count", "review_count")) and count is None:
            _warn(warnings, "invalid_review_window_count", f"index:{index}")
        if count is not None and count < 0:
            _warn(warnings, "negative_review_window_count", f"index:{index}")
            count = 0
        rating = _normalize_rating(_first(entry, _REVIEW_RATING_KEYS), _scope(entry, record),
                                   warnings, "window_")
        windows.append({"start": start, "end": end, "count": count, "rating": rating,
                        "index": index})
    return windows


def _collect_histogram(record: Mapping[str, Any], warnings: list[dict[str, str]]) -> list[dict[str, Any]] | None:
    """店铺自带的星级直方图：键为原始评分值，值为计数。"""
    raw = _first(record, _RATING_DIST_KEYS)
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        _warn(warnings, "invalid_rating_distribution", type(raw).__name__)
        return None
    entries: list[dict[str, Any]] = []
    for key, value in raw.items():
        label = re.sub(r"(?i)\s*(stars?|points?|★|星)$", "", str(key)).strip()
        rating = _normalize_rating(label, record, warnings, "hist_")
        count = _as_int(value)
        if rating is None:
            continue
        if count is None or count < 0:
            _warn(warnings, "invalid_rating_bucket_count", f"{key}:{value}")
            continue
        entries.append({"rating": rating, "count": count})
    return entries or None


# --------------------------------------------------------------------------- #
# 时间轴 / 空洞
# --------------------------------------------------------------------------- #
def _merge_intervals(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """合并相接或重叠的区间（相邻零窗口视为一段连续空档）。"""
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _merge_overlaps(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """只合并真正重叠的区间；首尾仅相接的区间保留边界（中间隔着一条评论）。"""
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if merged and start < merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _gap_payload(start: datetime, end: datetime, basis: str) -> dict[str, Any] | None:
    seconds = (end - start).total_seconds()
    if seconds <= 0:
        return None
    return {
        "start_utc": _iso_utc(start),
        "end_utc": _iso_utc(end),
        "seconds": int(seconds) if seconds == int(seconds) else seconds,
        "basis": basis,
    }


def _subtract(intervals: list[tuple[datetime, datetime]],
              cutters: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """从 intervals 中裁剪掉 cutters 已覆盖的区间，避免同一空档重复出现。"""
    if not cutters:
        return list(intervals)
    result: list[tuple[datetime, datetime]] = []
    for start, end in intervals:
        pieces = [(start, end)]
        for cut_start, cut_end in cutters:
            next_pieces: list[tuple[datetime, datetime]] = []
            for piece_start, piece_end in pieces:
                if cut_end <= piece_start or cut_start >= piece_end:
                    next_pieces.append((piece_start, piece_end))
                    continue
                if piece_start < cut_start:
                    next_pieces.append((piece_start, min(cut_start, piece_end)))
                if cut_end < piece_end:
                    next_pieces.append((max(cut_end, piece_start), piece_end))
            pieces = next_pieces
        result.extend(pieces)
    return result


def _no_review_windows(windows: list[dict[str, Any]], placed_reviews: list[dict[str, Any]],
                       coverage_start: datetime | None, coverage_end: datetime | None,
                       events_complete: bool, reported_count: int | None) -> list[dict[str, Any]]:
    """只在证据充分时推导"无评论区间"。

    证据只有两种：显式零评论窗口，或声明评论事件完整后的覆盖区间空档。
    稀疏评论列表本身不能证明无评论，因此不推导；两者重叠时以显式窗口为准，
    推导区间只保留显式窗口未覆盖的部分，既不重复也不丢失。
    """
    explicit = _merge_intervals(
        [(w["start"]["_dt"], w["end"]["_dt"]) for w in windows if w["count"] == 0]
    )

    derived: list[tuple[datetime, datetime]] = []
    if events_complete:
        points = sorted(item["ts"]["_dt"] for item in placed_reviews)
        if points:
            cursor = coverage_start or points[0]
            for point in points:
                derived.append((cursor, point))
                cursor = max(cursor, point)
            if coverage_end:
                derived.append((cursor, coverage_end))
        elif coverage_start is not None and coverage_end is not None and reported_count in (None, 0):
            derived.append((coverage_start, coverage_end))
    derived = _subtract(_merge_overlaps(derived), explicit)

    gaps: list[dict[str, Any]] = []
    for start, end in explicit:
        payload = _gap_payload(start, end, "explicit_zero_window")
        if payload:
            gaps.append(payload)
    for start, end in derived:
        payload = _gap_payload(start, end, "complete_events")
        if payload:
            gaps.append(payload)
    gaps.sort(key=lambda item: (item["start_utc"], item["end_utc"], item["basis"]))
    return gaps


def _star_distribution(reviews: list[dict[str, Any]], histogram: list[dict[str, Any]] | None,
                       windows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """半星直方图：优先逐条评论，其次店铺直方图，最后时间窗口，避免重复计数。"""
    counts = {f"{Decimal(step) * HALF_STEP:.1f}": 0 for step in range(11)}
    rated_reviews = [item["rating"] for item in reviews if item["rating"] is not None]
    if rated_reviews:
        source = "reviews"
        weighted_sources = [(rating, 1) for rating in rated_reviews]
    elif histogram:
        source = "histogram"
        weighted_sources = [(bucket["rating"], bucket["count"]) for bucket in histogram]
    else:
        source = "windows"
        weighted_sources = [(window["rating"], window["count"]) for window in windows
                            if window["rating"] is not None and window["count"] is not None]
    total = 0
    weighted = Decimal("0")
    for rating, weight in weighted_sources:
        key = f"{rating['stars']:.1f}"
        counts[key] += weight
        weighted += Decimal(str(rating["stars"])) * weight
        total += weight
    if total == 0:
        return None
    mean = (weighted / total).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    return {"source": source, "counts": counts, "total": total, "mean_stars": float(mean)}


def _timeline_events(release: dict[str, Any] | None, observed: dict[str, Any] | None,
                     version_name: str | None, reviews: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    """构造统一时间轴事件。评论过多时截断评论，但始终保留版本发布与观测两个锚点。"""
    anchors: list[dict[str, Any]] = []
    if release is not None:
        anchors.append({"kind": "release", "at_utc": release["utc"], "version": version_name,
                        "assumed_timezone": release["assumed_timezone"]})
    if observed is not None:
        anchors.append({"kind": "observation", "at_utc": observed["utc"],
                        "assumed_timezone": observed["assumed_timezone"]})

    review_events: list[dict[str, Any]] = []
    for item in reviews:
        review_events.append({
            "kind": "review",
            "at_utc": item["ts"]["utc"],
            "id": item["id"] if item["id"] is not None else item["identity"],
            "rating_stars": item["rating"]["stars"] if item["rating"] else None,
            "late": bool(item["late_reasons"]),
            "late_reasons": list(item["late_reasons"]),
            "assumed_timezone": item["ts"]["assumed_timezone"],
        })
    truncated = len(review_events) > MAX_EVENTS
    review_events = review_events[:MAX_EVENTS]

    events = anchors + review_events
    order = {"release": 0, "observation": 2, "review": 1}
    events.sort(key=lambda event: (event["at_utc"], order[event["kind"]],
                                   str(event.get("id", ""))))
    return events, truncated


# --------------------------------------------------------------------------- #
# 对外入口
# --------------------------------------------------------------------------- #
def _empty_diagnostics() -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "status": "passthrough",
        "store": None,
        "version": None,
        "observation_at_utc": None,
        "rating": None,
        "review_count": {
            "reported": None,
            "observed": None,
            "duplicates": 0,
            "late": 0,
            "late_ids": [],
            "basis": "none",
        },
        "star_distribution": None,
        "timeline": {
            "release_at_utc": None,
            "observation_at_utc": None,
            "first_review_at_utc": None,
            "last_review_at_utc": None,
            "release_to_observation_seconds": None,
            "timezone": "UTC",
            "timezone_source": "default",
            "assumed_timezone": False,
            "events": [],
            "events_truncated": False,
        },
        "no_review_windows": [],
        "gaps_computable": False,
        "warnings": [],
    }


def _version_info(record: Mapping[str, Any]) -> tuple[str | None, Mapping[str, Any] | None, Any]:
    version_value = record.get("version")
    if isinstance(version_value, Mapping):
        name = version_value.get("name", version_value.get("version", version_value.get("id")))
        release = _first(version_value, _RELEASE_KEYS)
        if release is None:
            release = _first(version_value, ("ts", "timestamp"))
        return (str(name) if name is not None else None), version_value, release
    if version_value is not None and not _is_blank(version_value):
        return str(version_value), None, None
    name = record.get("version_name", record.get("app_version"))
    if name is None or _is_blank(name):
        return None, None, None
    return str(name), None, None


def _reported_count(record: Mapping[str, Any], warnings: list[dict[str, str]]) -> int | None:
    raw = _first(record, _REVIEW_COUNT_KEYS)
    if raw is None or _is_blank(raw):
        return None
    count = _as_int(raw)
    if count is None:
        _warn(warnings, "invalid_review_count", str(raw))
        return None
    if count < 0:
        _warn(warnings, "negative_review_count", str(count))
        return None
    return count


def _align(record: Mapping[str, Any]) -> dict[str, Any]:
    warnings: list[dict[str, str]] = []
    diag = _empty_diagnostics()

    default_tz, tz_name, tz_assumed = _resolve_timezone(record, warnings)
    diag["timeline"]["timezone"] = tz_name
    diag["timeline"]["timezone_source"] = "default" if tz_assumed else "declared"

    version_name, _version_block, block_release = _version_info(record)
    # 版本块内的发布时间更具体（它就是"这个版本的发布时间"），优先于记录级字段。
    if block_release is not None:
        release = _parse_time(block_release, default_tz, warnings, "release")
        record_release = _parse_time(_first(record, _RELEASE_KEYS), default_tz, warnings,
                                     "record.release")
        if (release is not None and record_release is not None
                and release["_dt"] != record_release["_dt"]):
            _warn(warnings, "release_time_conflict",
                  f"version:{release['utc']}:record:{record_release['utc']}")
    else:
        release = _parse_time(_first(record, _RELEASE_KEYS), default_tz, warnings, "release")
    observed = _parse_time(_first(record, _OBSERVED_KEYS), default_tz, warnings, "observed")

    reported_count = _reported_count(record, warnings)
    raw_rating_value, _ = _rating_parts(_first(record, _RATING_KEYS))
    raw_numeric_rating = _as_decimal(raw_rating_value)
    rating: dict[str, Any] | None = None
    if raw_numeric_rating is not None and reported_count == 0 and raw_numeric_rating == 0:
        _warn(warnings, "rating_without_reviews", "0 treated as no rating")
    else:
        if raw_numeric_rating is not None and reported_count == 0:
            _warn(warnings, "rating_without_reviews", f"reported 0 but rating {raw_rating_value}")
        rating = _normalize_rating(_first(record, _RATING_KEYS), record, warnings, "")

    threshold = _as_decimal(_first(record, _LATE_THRESHOLD_KEYS))
    if threshold is not None and threshold < 0:
        _warn(warnings, "negative_late_threshold", str(threshold))
        threshold = Decimal("0")
    reviews = _collect_reviews(record, default_tz, warnings, threshold,
                               observed["_dt"] if observed else None)
    windows = _collect_windows(record, default_tz, warnings)
    histogram = _collect_histogram(record, warnings)

    completeness = _first(record, _COMPLETENESS_KEYS)
    events_complete = _is_true(completeness) or (
        isinstance(completeness, str) and completeness.strip().lower() in _COMPLETE_VALUES
    )

    # 可观测评论数量：逐条（去重后）> 直方图 > 窗口 > 仅声明。
    observed_count: int | None = None
    basis = "none"
    if reviews["present"]:
        basis = "individual"
        observed_count = len(reviews["items"])
    elif histogram:
        basis = "histogram"
        observed_count = sum(bucket["count"] for bucket in histogram)
    elif windows:
        basis = "windows"
        counts = [w["count"] for w in windows if w["count"] is not None]
        observed_count = sum(counts) if len(counts) == len(windows) else None
    elif reported_count is not None:
        basis = "aggregate"

    # 分页抽样时 observed < reported 属正常；只有"观察量超过声明量"或"声明事件完整却
    # 对不上"才是真矛盾，才值得告警。
    if reported_count is not None and observed_count is not None:
        if observed_count > reported_count:
            _warn(warnings, "review_count_exceeds_reported",
                  f"reported:{reported_count}:observed:{observed_count}")
        elif observed_count < reported_count and events_complete:
            _warn(warnings, "review_count_shortfall",
                  f"reported:{reported_count}:observed:{observed_count}")

    distribution = _star_distribution(reviews["items"], histogram, windows)
    full_population = (
        reported_count is None
        or (observed_count is not None and observed_count == reported_count)
        or events_complete
    )
    if distribution and rating and distribution["source"] != "histogram" and full_population:
        if abs(distribution["mean_stars"] - rating["stars"]) > 0.5:
            _warn(warnings, "mean_rating_mismatch",
                  f"aggregate:{rating['stars']}:distribution:{distribution['mean_stars']}")

    # 版本与观测时间倒挂、评论晚于观测，都只影响该条诊断，不丢弃数据。
    placed = reviews["placed"]
    release_dt = release["_dt"] if release else None
    observed_dt = observed["_dt"] if observed else None
    first_review = placed[0]["ts"]["_dt"] if placed else None
    last_review = placed[-1]["ts"]["_dt"] if placed else None
    if release_dt and observed_dt and release_dt > observed_dt:
        _warn(warnings, "release_after_observation",
              f"{_iso_utc(release_dt)}>{_iso_utc(observed_dt)}")

    has_signal = any((
        rating is not None,
        reported_count is not None,
        observed_count is not None,
        release is not None,
        observed is not None,
        version_name is not None,
        bool(windows),
        bool(histogram),
    ))
    if not has_signal:
        # 没有任何可对齐信号：记录透明透传；若输入里本有字段但全部解析失败，
        # 用 recovered 表明"看过但没得出可用结果"，避免把脏数据误报成正常透传。
        diag["status"] = "recovered" if warnings else "passthrough"
        diag["warnings"] = _sorted_warnings(warnings)
        return diag

    # ``assumed_timezone`` 只表示"某个时间点缺偏移、按声明/缺省时区解释"；
    # 显式带偏移（含 Z）的时间点不算假设。
    assumed = any(
        parsed is not None and parsed["assumed_timezone"]
        for parsed in [release, observed]
    ) or any(item["ts"]["assumed_timezone"] for item in placed)

    events, truncated = _timeline_events(release, observed, version_name, placed)

    diag["status"] = "recovered" if warnings else "aligned"
    diag["store"] = str(record.get("store") or record.get("src") or "") or None
    diag["version"] = {"name": version_name, "release": _public_time(release)}
    diag["observation_at_utc"] = _iso_utc(observed_dt) if observed_dt else None
    diag["rating"] = rating
    diag["review_count"] = {
        "reported": reported_count,
        "observed": observed_count,
        "duplicates": reviews["duplicates"],
        "late": len(reviews["late_items"]),
        "late_ids": sorted(
            str(item["id"]) if item["id"] is not None else item["identity"]
            for item in reviews["late_items"]
        ),
        "basis": basis,
    }
    diag["star_distribution"] = distribution

    release_to_obs = None
    if release_dt and observed_dt:
        release_to_obs = int((observed_dt - release_dt).total_seconds())
    diag["timeline"] = {
        "release_at_utc": _iso_utc(release_dt) if release_dt else None,
        "observation_at_utc": _iso_utc(observed_dt) if observed_dt else None,
        "first_review_at_utc": _iso_utc(first_review) if first_review else None,
        "last_review_at_utc": _iso_utc(last_review) if last_review else None,
        "release_to_observation_seconds": release_to_obs,
        "timezone": tz_name,
        "timezone_source": "default" if tz_assumed else "declared",
        "assumed_timezone": assumed,
        "events": events,
        "events_truncated": truncated,
    }

    # 覆盖区间 = 所有已知时间点的最小/最大值，供"完整事件"推导空档。
    points = [x for x in (release_dt, observed_dt, first_review, last_review) if x is not None]
    if windows:
        points.extend([windows[0]["start"]["_dt"], windows[-1]["end"]["_dt"]])
    cover_start = min(points) if points else None
    cover_end = max(points) if points else None

    gaps = _no_review_windows(windows, placed, cover_start, cover_end,
                              events_complete, reported_count)
    diag["no_review_windows"] = gaps
    diag["gaps_computable"] = bool(
        any(w["count"] == 0 for w in windows)
        or (events_complete and cover_start is not None)
    )

    diag["warnings"] = _sorted_warnings(warnings)
    return diag


def process(record: Mapping[str, Any]) -> dict[str, Any]:
    """对齐单条记录的评分时间轴；返回新 dict，不修改输入。

    接口与基线一致：``process(record) -> dict``。原有字段逐字段保留，派生诊断写入
    ``_score_align``；``_stage_score_align`` 仅在缺失时补写，已有值不覆盖。
    """
    if not isinstance(record, Mapping):
        raise TypeError(f"score_align.process expects a mapping, got {type(record).__name__}")

    try:
        # 只做浅拷贝：本模块从不修改嵌套结构，避免为大量评论复制一份全量内存。
        rec = dict(record)
    except Exception:  # pragma: no cover - 防御性回退
        rec = dict(record.items())

    try:
        diag = _align(record)
    except Exception as exc:  # 失败恢复：单条脏数据不打断流水线。
        diag = _empty_diagnostics()
        diag["status"] = "recovered"
        _warn(diag["warnings"], "internal_error", type(exc).__name__)
        diag["warnings"] = _sorted_warnings(diag["warnings"])

    # 保持基线字段值不变（旧输入逐字段兼容）；真实状态见 ``_score_align.status``。
    rec.setdefault("_stage_score_align", "naive")
    rec["_score_align"] = diag
    return rec
