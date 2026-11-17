"""单一职责：版本谱系（versionline）。

只处理自己的记录语义，不做抓取、编排或落库决定：
- 把不同商店（src）的版本字符串、构建号、预发布标签、发布日期
  还原成可比较的版本谱系（解析、规范化、排序键、置信度、来源/溯源）；
- 处理同一版本多商店先后出现、回滚版本、缺失构建号、非法标签、
  时区边界等输入；
- 为每条归属保留来源（source / source_id / source_seq）与置信信息
  （confidence / confidence_reasons / 各类 flag）；
- 跨记录的时序排序、回滚标记与再观察标记由本模块的 `build_lineage`
  纯函数提供，编排层可在任意时刻从已落库记录重建谱系（重启恢复），
  不依赖进程内的可变状态。

输入兼容两种格式：
- 现行字段：app/version/build/prerelease/released_at；
- 旧格式字段（别名，自动识别并标记 `legacy_format`）：
  app_name/ver/version_code/release_tag/published_at。

对外接口（保持与其它阶段一致的单记录 process 约定）：
- process(record) -> record：纯函数，输入不变，返回带 `version_line`
  与规范化字段的副本；对不含版本信息的记录只打阶段标记并透传。
- build_lineage(records) -> list[dict]：从一批记录重建可比较的版本谱系。
两个函数均确定性、无副作用，同输入永远同输出（幂等键稳定）。
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

STAGE = "ok"

# 旧格式字段别名 -> 现行字段
_ALIASES = {
    "app_name": "app",
    "ver": "version",
    "version_code": "build",
    "release_tag": "prerelease",
    "published_at": "released_at",
}

# 旧格式预发布标签的常见拼写 -> 标准 SemVer 预发布标识
_LEGACY_TAGS = {
    "alpha": "alpha",
    "beta": "beta",
    "rc": "rc",
    "preview": "rc",
    "pre": "rc",
    "snapshot": "alpha",
    "dev": "dev",
    "daily": "dev",
    "nightly": "dev",
}

# 预发布标识的已知合法名（其它拼写视为非法标签，仍保留原值但降置信）
_KNOWN_TAGS = {"dev", "alpha", "beta", "rc"}

# 预发布标签段：字母 [0-9A-Za-z-]* 或数字（无前导零）
_TAG_SEGMENT = re.compile(r"^(?:0|[1-9]\d*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)$")
_NUMERIC = re.compile(r"^(0|[1-9]\d*)$")
# 版本核心：MAJOR.MINOR.PATCH，每段无前导零的纯数字
_CORE_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
# 旧格式宽松版本：1~4 段数字，允许 v/V 前缀（如 v1、v1.2、1.2.3.4）
_LOOSE_RE = re.compile(r"^[vV]?(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:\.(\d+))?$")
# 构建号：允许商店常用的纯数字写法（含前导零，按整数比较）
_BUILD_RE = re.compile(r"^\d+$")
# 非空、无空白的标签字符校验（识别非法字符）
_LABEL_CHARS = re.compile(r"^[0-9A-Za-z.-]+$")


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------

def _first(record: dict, key: str) -> tuple[Any, bool]:
    """返回 (值, 是否来自旧格式别名字段)。"""
    if key in record and record[key] not in (None, ""):
        return record[key], False
    alias = next((a for a, canon in _ALIASES.items() if canon == key), None)
    if alias and alias in record and record[alias] not in (None, ""):
        return record[alias], True
    return None, False


def _normalize_tag(raw: str, legacy: bool) -> tuple[str | None, bool, bool]:
    """规范化单个预发布标签。

    返回 (标准标签或原值, 标签是否非法, 是否被旧格式改写)。
    非法判定：含空白/非法字符、数字段前导零，或标签名不在已知集合。
    """
    tag = raw.strip()
    illegal = False
    rewritten = False
    if not tag:
        return None, illegal, rewritten
    if not _LABEL_CHARS.match(tag):
        illegal = True
        return tag, illegal, rewritten
    low = tag.lower()
    if legacy and low in _LEGACY_TAGS:
        canonical = _LEGACY_TAGS[low]
        rewritten = canonical != tag
        tag = canonical
        low = canonical
    for seg in tag.split("."):
        if not _TAG_SEGMENT.match(seg):
            illegal = True
            break
        if _NUMERIC.match(seg) and len(seg) > 1 and seg.startswith("0"):
            illegal = True
            break
    if low.split(".")[0] not in _KNOWN_TAGS:
        illegal = True
    return tag, illegal, rewritten


def _parse_version(raw_version: str, raw_prerelease: Any,
                   legacy: bool) -> dict[str, Any]:
    """解析版本字符串 + 预发布标签，产出规范化版本与可比较排序键。"""
    text = str(raw_version).strip()
    reasons: list[str] = []
    core_valid = False
    core = (0, 0, 0)
    pre: list[str] | None = None
    pre_illegal = False
    pre_rewritten = False

    # 先剥离版本字符串内嵌的 -prerelease，再匹配版本核心
    head, _, embedded_tail = text.partition("-")
    m = _CORE_RE.match(head)
    if m:
        core = tuple(int(g) for g in m.groups())
        core_valid = True
    else:
        lm = _LOOSE_RE.match(head)
        if lm:
            groups = lm.groups()
            # 统一到 MAJOR.MINOR.PATCH 三元组；第四段（商店常见）截断并留痕
            core = tuple(int(g) if g is not None else 0 for g in groups[:3])
            core_valid = True
            reasons.append("legacy_version_shape")
            if groups[3] is not None:
                reasons.append("legacy_four_part_version_truncated")
        else:
            reasons.append("unparseable_version")

    # 预发布标签：优先独立字段，其次版本字符串内嵌的 -prerelease
    embedded_pre = embedded_tail if embedded_tail else None
    if raw_prerelease not in (None, ""):
        tag, pre_illegal, pre_rewritten = _normalize_tag(str(raw_prerelease), legacy)
        if tag is not None:
            pre = tag.split(".")
    elif embedded_pre:
        tag, pre_illegal, pre_rewritten = _normalize_tag(embedded_pre, legacy)
        if tag is not None:
            pre = tag.split(".")
    if pre_illegal:
        reasons.append("illegal_prerelease_label")
    if pre_rewritten:
        reasons.append("legacy_tag_normalized")

    if pre is None:
        canonical = "%d.%d.%d" % core if core_valid else text
    else:
        canonical = "%d.%d.%d-%s" % (core + (".".join(pre),))

    sort_key = _build_sort_key(core_valid, core, pre, pre_illegal, text)
    return {
        "canonical": canonical,
        "core": list(core),
        "prerelease": pre,
        "core_valid": core_valid,
        "prerelease_illegal": pre_illegal,
        "sort_key": sort_key,
        "parse_reasons": reasons,
    }


def _build_sort_key(core_valid: bool, core: tuple[int, int, int],
                    pre: list[str] | None, pre_illegal: bool,
                    raw: str) -> list[Any]:
    """构造可 JSON 序列化、可逐段比较的总排序键（含非法标签兜底）。

    层级：有效核心组 → 核心三元组 → 发布类型（正式版 > 预发布）→
    预发布段（数字按数值，字母按字典序）→ 非法标签降级 →
    非法版本的原始字符串兜底。
    """
    key: list[Any] = [1 if core_valid else 0]
    key.append(list(core) if core_valid else [0, 0, 0])
    if pre is None:
        key.append(1)          # 正式发布高于同核心的预发布
        key.append([])
    else:
        key.append(0)
        segments: list[list[Any]] = []
        for seg in pre:
            if _NUMERIC.match(seg):
                segments.append([0, int(seg), ""])   # 数字段排在字母段前
            else:
                segments.append([1, 0, seg])
        key.append(segments)
    # 非法标签整体降级到同核心合法预发布之后
    key.append(1 if pre_illegal else 0)
    key.append(raw if not core_valid else "")
    return key


def _parse_build(raw_build: Any) -> tuple[int | None, bool, bool]:
    """返回 (构建号或 None, 是否缺失, 是否非法)。"""
    if raw_build in (None, ""):
        return None, True, False
    text = str(raw_build).strip()
    if _BUILD_RE.match(text):
        return int(text), False, False
    return None, False, True


def _parse_released_at(raw: Any) -> tuple[int | None, str | None, bool, bool]:
    """把发布时间统一为 UTC epoch 秒与 UTC ISO 字符串。

    支持 'Z' 结尾与显式偏移；无时区信息视为 UTC 并标记 naive。
    返回 (epoch_utc, iso_utc, 是否解析失败, 是否为无时区朴素时间)。
    """
    if raw in (None, ""):
        return None, None, True, False
    text = str(raw).strip()
    candidate = text
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None, None, True, False
    naive = dt.tzinfo is None
    if naive:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    epoch = int(dt_utc.timestamp())
    iso = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    return epoch, iso, False, naive


def _short_hash(parts: list[str]) -> str:
    blob = "|".join(parts)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _confidence(reasons: list[str], *, missing_build: bool, bad_build: bool,
                missing_time: bool, illegal_pre: bool, legacy: bool) -> float:
    """确定性置信度：从 1.0 按可解释的原因扣分，下限 0.1。"""
    score = 1.0
    if "unparseable_version" in reasons:
        score -= 0.5
    if "legacy_version_shape" in reasons:
        score -= 0.1
    if illegal_pre:
        score -= 0.25
    if bad_build:
        score -= 0.15
    if missing_build:
        score -= 0.1
    if missing_time:
        score -= 0.1
    if legacy:
        score -= 0.05
    return round(max(0.1, score), 2)


# ---------------------------------------------------------------------------
# 对外接口
# ---------------------------------------------------------------------------

def process(record: dict) -> dict:
    """单记录版本语义变换（纯函数、确定性、幂等）。

    不含版本字段的记录只透传并打阶段标记；含版本字段的记录会得到
    `version_line`（规范化版本、排序键、置信度、来源与幂等键等）。
    """
    rec = dict(record)
    rec.setdefault("_stage_versionline", STAGE)

    raw_version, v_legacy = _first(rec, "version")
    if raw_version in (None, ""):
        # 非版本类记录：不干预下游，仅保留阶段可观测标记
        return rec

    raw_app, a_legacy = _first(rec, "app")
    raw_build, b_legacy = _first(rec, "build")
    raw_pre, p_legacy = _first(rec, "prerelease")
    raw_time, t_legacy = _first(rec, "released_at")
    legacy = any((v_legacy, a_legacy, b_legacy, p_legacy, t_legacy))

    app = str(raw_app).strip() if raw_app not in (None, "") else ""
    parsed = _parse_version(str(raw_version), raw_pre, legacy)
    build, missing_build, bad_build = _parse_build(raw_build)
    epoch, iso_utc, missing_time, naive_time = _parse_released_at(raw_time)

    reasons = list(parsed["parse_reasons"])
    if missing_build:
        reasons.append("missing_build")
    if bad_build:
        reasons.append("illegal_build")
    if missing_time:
        reasons.append("missing_or_bad_released_at")
    if naive_time:
        reasons.append("naive_timestamp_assumed_utc")
    if legacy:
        reasons.append("legacy_format")

    canonical = parsed["canonical"]
    version_key = _short_hash([app, canonical, "" if build is None else str(build)])
    attribution_key = _short_hash([version_key, str(rec.get("src", "")),
                                   str(rec.get("id", ""))])
    confidence = _confidence(
        reasons,
        missing_build=missing_build,
        bad_build=bad_build,
        missing_time=missing_time,
        illegal_pre=parsed["prerelease_illegal"],
        legacy=legacy,
    )

    # 规范化后的扁平字段（现行字段名），原始字段保持不动以便溯源
    rec["app"] = app
    rec["version"] = canonical
    if build is not None:
        rec["build"] = build
    rec["released_at_utc"] = iso_utc
    rec["released_epoch"] = epoch

    rec["version_line"] = {
        "app": app,
        "canonical_version": canonical,
        "version_core": parsed["core"],
        "core_valid": parsed["core_valid"],
        "prerelease": parsed["prerelease"],
        "prerelease_illegal": parsed["prerelease_illegal"],
        "build": build,
        "build_missing": missing_build,
        "build_illegal": bad_build,
        "released_at_utc": iso_utc,
        "released_epoch": epoch,
        "released_at_missing": missing_time,
        "released_at_naive": naive_time,
        "sort_key": parsed["sort_key"],
        "confidence": confidence,
        "confidence_reasons": reasons,
        "legacy_format": legacy,
        "source": str(rec.get("src", "")),
        "source_id": rec.get("id"),
        "source_seq": rec.get("seq"),
        "version_key": version_key,
        "attribution_key": attribution_key,
    }
    return rec


def build_lineage(records: list[dict]) -> list[dict]:
    """从一批记录重建可比较的版本谱系（纯函数，可在重启后反复调用）。

    只接受含 `version_line` 的记录（即经过 process 的归属记录）。
    排序按 app、发布时刻（UTC）、版本排序键、构建号、来源、来源 id
    做全确定性决胜；逐应用扫描发布序列，标记：
    - rollback：当前版本低于本应用时序上的直接前驱（同版再观察不算回滚；
      回滚后的正常迭代只把首次下降标记为回滚）；
    - reobserved：同一 version_key 在更早的归属中已出现（多商店先后上架）。
    返回新列表，不修改输入。
    """
    enriched = []
    for rec in records:
        vl = rec.get("version_line")
        if not vl:
            continue
        enriched.append((dict(rec), vl))

    def order(item: tuple[dict, dict]) -> tuple:
        rec, vl = item
        epoch = vl.get("released_epoch")
        return (
            vl.get("app", ""),
            epoch if epoch is not None else 1 << 62,   # 缺失时间排最后
            vl.get("sort_key", []),
            vl.get("build") if vl.get("build") is not None else -1,
            vl.get("source", ""),
            str(vl.get("source_id", "")),
        )

    enriched.sort(key=order)

    lineage: list[dict] = []
    seen_versions: dict[str, dict] = {}      # app -> version_key -> 首次归属
    prev_key: dict[str, list[Any]] = {}      # app -> 上一条发布的排序键（局部前驱）
    per_app_index: dict[str, int] = {}
    for rec, vl in enriched:
        app = vl.get("app", "")
        out = dict(rec)
        out_vl = dict(vl)

        reobserved = False
        first = seen_versions.setdefault(app, {})
        vk = vl.get("version_key")
        if vk in first:
            reobserved = True
            out_vl["first_seen_source"] = first[vk].get("source")
            out_vl["first_seen_id"] = first[vk].get("source_id")
        else:
            first[vk] = vl

        # 回滚：低于该应用时序上的直接前驱版本（同版再观察不算回滚）。
        # 回滚后的正常迭代（如 2.0.0→1.5.0→1.6.0）只把首次下降标记为回滚。
        sk = vl.get("sort_key", [])
        rollback = app in prev_key and _compare_keys(sk, prev_key[app]) < 0
        prev_key[app] = sk

        idx = per_app_index.get(app, 0)
        per_app_index[app] = idx + 1
        out_vl["rollback"] = rollback
        out_vl["reobserved"] = reobserved
        out_vl["lineage_index"] = idx
        out["version_line"] = out_vl
        lineage.append(out)
    return lineage


def _compare_keys(a: list[Any], b: list[Any]) -> int:
    """对 _build_sort_key 产出的嵌套列表做字典序比较，返回 -1/0/1。"""
    if a == b:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    if isinstance(a, list) and isinstance(b, list):
        for x, y in zip(a, b):
            cmp = _compare_keys(x, y)
            if cmp:
                return cmp
        return (len(a) > len(b)) - (len(a) < len(b))
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return (a > b) - (a < b)
    sa, sb = str(a), str(b)
    return (sa > sb) - (sa < sb)
