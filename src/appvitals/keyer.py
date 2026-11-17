"""单一职责：派生稳定主键。

不变量（跨模块与 dedup 共享 identity）：
- 同一身份（同 app/review/version/score 身份元组）永远映射到同一存储键；
  因此编辑、删除、回滚、重传只会 upsert 同一行，物理上不可能出现幽灵行。
- 历史无类型记录（只有 id 的 event 形态）保持基线键值不变（锁定旧输出）。
- 完全无法确定身份的记录退化为整体内容哈希（仍确定性、可重复），由 dedup 决定隔离。
"""
import hashlib, json
from . import identity as _id


def key_of(record):
    rid = record.get("id")
    kind = str(record.get("kind") or "").strip().lower()
    if not kind and rid:
        # 历史路径：基线行为不变
        return str(rid)
    try:
        return _id.identity_fp(record)
    except _id.IdentityError:
        return hashlib.sha256(
            json.dumps(record, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:16]
