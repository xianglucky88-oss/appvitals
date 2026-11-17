"""单一职责：本地 sqlite3 持久化。

既有职责（保持不变，锁定旧输出）：
- records 表 + 幂等 upsert / count / all_records。

新增职责（dedup 状态与诊断，全部独立建表，不改 records 结构）：
- dedup_index：每个身份一行的去重状态（内容指纹、来源、时间戳、墓碑、存储键）。
- dedup_audit：append-only 决策审计，只存指纹与决策码，不存敏感原始数据。
- dedup_quarantine：不可重试坏记录隔离区，只存 payload 摘要与字段形状。
- dedup_meta：规则版本/迁移水位。
"""
import json, sqlite3
from . import config
from .identity import redacted_preview

def connect():
    con = sqlite3.connect(config.DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    _create_dedup_tables(con)
    return con

def _create_dedup_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS dedup_index(
            identity_fp   TEXT PRIMARY KEY,
            kind          TEXT NOT NULL,
            store_key     TEXT NOT NULL,
            content_fp    TEXT NOT NULL,
            src           TEXT,
            src_rank      INTEGER NOT NULL,
            event_ts      TEXT,
            is_tombstone  INTEGER NOT NULL DEFAULT 0,
            rule_version  INTEGER NOT NULL,
            seen_count    INTEGER NOT NULL DEFAULT 1,
            updated_at    TEXT NOT NULL DEFAULT ''
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS dedup_audit(
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            identity_fp   TEXT NOT NULL,
            content_fp    TEXT NOT NULL,
            decision      TEXT NOT NULL,
            reason        TEXT,
            src           TEXT,
            rule_version  INTEGER NOT NULL,
            ts            TEXT NOT NULL
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS dedup_quarantine(
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_digest TEXT NOT NULL,
            reason         TEXT NOT NULL,
            field_shape    TEXT NOT NULL,
            kind           TEXT,
            ts             TEXT NOT NULL,
            UNIQUE(payload_digest, reason)
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS dedup_meta(
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )""")
    con.commit()

def upsert(con, record):
    con.execute("INSERT INTO records(key,data) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (record.key, json.dumps(record.data, ensure_ascii=False)))

def commit(con): con.commit()

def count(con): return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]

# ---------------------------------------------------------------- dedup 状态

def index_get(con, identity_fp):
    row = con.execute(
        "SELECT identity_fp, kind, store_key, content_fp, src, src_rank, event_ts, "
        "is_tombstone, rule_version, seen_count, updated_at FROM dedup_index WHERE identity_fp=?",
        (identity_fp,)).fetchone()
    if row is None:
        return None
    cols = ("identity_fp", "kind", "store_key", "content_fp", "src", "src_rank",
            "event_ts", "is_tombstone", "rule_version", "seen_count", "updated_at")
    return dict(zip(cols, row))

def index_upsert(con, *, identity_fp, kind, store_key, content_fp, src, src_rank,
                 event_ts, is_tombstone, rule_version, updated_at):
    con.execute("""
        INSERT INTO dedup_index(identity_fp, kind, store_key, content_fp, src, src_rank,
                                event_ts, is_tombstone, rule_version, seen_count, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,1,?)
        ON CONFLICT(identity_fp) DO UPDATE SET
            kind=excluded.kind, store_key=excluded.store_key, content_fp=excluded.content_fp,
            src=excluded.src, src_rank=excluded.src_rank, event_ts=excluded.event_ts,
            is_tombstone=excluded.is_tombstone, rule_version=excluded.rule_version,
            seen_count=dedup_index.seen_count+1, updated_at=excluded.updated_at
    """, (identity_fp, kind, store_key, content_fp, src, src_rank,
          event_ts, 1 if is_tombstone else 0, rule_version, updated_at))

def index_reanchor(con, *, identity_fp, content_fp, rule_version):
    """规则升级后只重置内容指纹基线，不动身份/存储键 => 不产生新行。"""
    con.execute(
        "UPDATE dedup_index SET content_fp=?, rule_version=? WHERE identity_fp=?",
        (content_fp, rule_version, identity_fp))

def index_rows(con):
    return con.execute("SELECT identity_fp, kind, store_key, content_fp, src, src_rank, "
                       "event_ts, is_tombstone, rule_version, seen_count, updated_at "
                       "FROM dedup_index ORDER BY identity_fp").fetchall()

def audit_append(con, *, identity_fp, content_fp, decision, reason, src, rule_version, ts):
    con.execute(
        "INSERT INTO dedup_audit(identity_fp, content_fp, decision, reason, src, rule_version, ts) "
        "VALUES(?,?,?,?,?,?,?)",
        (identity_fp, content_fp, decision, reason, src, rule_version, ts))

def audit_rows(con):
    return con.execute(
        "SELECT identity_fp, content_fp, decision, reason, src, rule_version, ts "
        "FROM dedup_audit ORDER BY id").fetchall()

def quarantine_append(con, *, payload_digest, reason, field_shape, kind, ts):
    con.execute("""
        INSERT INTO dedup_quarantine(payload_digest, reason, field_shape, kind, ts)
        VALUES(?,?,?,?,?)
        ON CONFLICT(payload_digest, reason) DO UPDATE SET
            field_shape=excluded.field_shape, kind=excluded.kind, ts=excluded.ts
    """, (payload_digest, reason, field_shape, kind, ts))

def quarantine_rows(con):
    return con.execute(
        "SELECT payload_digest, reason, field_shape, kind, ts FROM dedup_quarantine ORDER BY id").fetchall()

def meta_get(con, k, default=None):
    row = con.execute("SELECT v FROM dedup_meta WHERE k=?", (k,)).fetchone()
    return row[0] if row else default

def meta_set(con, k, v):
    con.execute("INSERT INTO dedup_meta(k,v) VALUES(?,?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))

def safe_shape(rec):
    """供 quarantine 落库的字段形状：键名+类型，值全部不落盘。"""
    return redacted_preview(rec, limit=2000)

def legacy_records(con):
    """历史数据迁移输入：records 表中尚未在 dedup_index 登记身份的行。"""
    return con.execute("""
        SELECT r.key, r.data FROM records r
        WHERE NOT EXISTS (SELECT 1 FROM dedup_index d WHERE d.store_key = r.key)
    """).fetchall()
