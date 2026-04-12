"""Postgres mirror for Jennifer's CRM writes — the hybrid write-through layer.

Every write to Jennifer's SQLite contacts.db gets mirrored to the unified
business_intel Postgres database. If Postgres is down or the mirror insert
fails, the operation is appended to a replay journal (~/.jennifer/pg_replay.jsonl)
and drained on the next successful connection.

Read paths stay on SQLite — this is write-only.

Design:
- SQLite is authoritative and always succeeds first
- Postgres is a mirror that should eventually reflect every SQLite state
- Replay journal is the "eventually" guarantee
- Module-level lazy connection; reconnects on failure

Tables mirrored:
    contacts     → business_intel.contacts  (+ auto-create businesses.id)
    audits       → business_intel.audits
    research     → business_intel.research
    interactions → business_intel.interactions
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional

logger = logging.getLogger("jennifer.pg_mirror")

try:
    import psycopg
    HAVE_PSYCOPG = True
except ImportError:
    HAVE_PSYCOPG = False
    psycopg = None  # type: ignore

# ----------------------------------------------------------------------------
# configuration
# ----------------------------------------------------------------------------
PG_DSN = os.environ.get("BUSINESS_INTEL_DSN", "postgresql:///business_intel")
JENNIFER_DIR = Path.home() / ".jennifer"
REPLAY_JOURNAL = JENNIFER_DIR / "pg_replay.jsonl"
MIRROR_ENABLED = os.environ.get("JENNIFER_PG_MIRROR", "1") == "1"

_conn: Optional["psycopg.Connection"] = None


# ----------------------------------------------------------------------------
# connection management
# ----------------------------------------------------------------------------
def _get_conn() -> Optional["psycopg.Connection"]:
    """Return a live Postgres connection, or None if unavailable.

    Cached at module level; reconnects lazily on failure.
    """
    global _conn
    if not HAVE_PSYCOPG or not MIRROR_ENABLED:
        return None
    if _conn is not None:
        try:
            _conn.execute("SELECT 1")
            return _conn
        except Exception:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
    try:
        _conn = psycopg.connect(PG_DSN, connect_timeout=2)
        return _conn
    except Exception as e:
        logger.debug("pg_mirror: cannot connect to Postgres: %s", e)
        return None


# ----------------------------------------------------------------------------
# replay journal
# ----------------------------------------------------------------------------
def _append_replay(op: str, payload: dict, sqlite_id: Optional[int] = None,
                   error: str = "") -> None:
    """Append a failed op to the replay journal so it can be retried later."""
    JENNIFER_DIR.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "op": op,
        "sqlite_id": sqlite_id,
        "payload": payload,
        "error": error,
    }
    with REPLAY_JOURNAL.open("a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def replay_pending() -> tuple[int, int]:
    """Drain the replay journal. Returns (drained, still_pending)."""
    if not REPLAY_JOURNAL.exists():
        return 0, 0
    conn = _get_conn()
    if conn is None:
        # Still can't reach Postgres — count the pending ones and exit
        pending = sum(1 for _ in REPLAY_JOURNAL.open())
        return 0, pending

    entries = [json.loads(line) for line in REPLAY_JOURNAL.open() if line.strip()]
    drained = 0
    still_pending: list[dict] = []
    for entry in entries:
        op = entry["op"]
        payload = entry["payload"]
        try:
            handler = _OP_HANDLERS.get(op)
            if handler is None:
                logger.warning("pg_mirror: unknown replay op %s", op)
                still_pending.append(entry)
                continue
            handler(conn, payload)
            conn.commit()
            drained += 1
        except Exception as e:
            conn.rollback()
            logger.warning("pg_mirror: replay failed for %s: %s", op, e)
            entry["error"] = str(e)
            still_pending.append(entry)

    # Rewrite the journal with only the still-pending entries
    if still_pending:
        with REPLAY_JOURNAL.open("w") as f:
            for entry in still_pending:
                f.write(json.dumps(entry, default=str) + "\n")
    else:
        REPLAY_JOURNAL.unlink(missing_ok=True)

    return drained, len(still_pending)


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
@contextmanager
def _cursor() -> Iterator[Optional["psycopg.Cursor"]]:
    """Yield a cursor on a live Postgres connection, or None if unavailable."""
    conn = _get_conn()
    if conn is None:
        yield None
        return
    cur = conn.cursor()
    try:
        yield cur
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        cur.close()


def _parse_tags(tags_json: str) -> list:
    if not tags_json:
        return []
    try:
        result = json.loads(tags_json)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _parse_ts(value) -> Optional[datetime]:
    if value is None or value == "":
        return None
    s = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s.replace("Z", ""), fmt)
        except ValueError:
            continue
    return None


def _upsert_business(cur, *, name: str, address: str = "", city: str = "",
                     state: str = "", website: str = "", phone: str = "",
                     category: str = "") -> Optional[str]:
    """Reuse the business_intel dedup logic to find-or-create a business.

    Returns the UUID as string, or None if name is empty.
    """
    if not name:
        return None
    import hashlib
    import re

    def _norm(s: str) -> str:
        s = (s or "").lower().strip()
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s.strip()

    raw = "|".join([_norm(name), _norm(address), _norm(city), _norm(state)])
    dedup_key = hashlib.sha256(raw.encode()).hexdigest()

    cur.execute("SELECT id FROM businesses WHERE dedup_key = %s", (dedup_key,))
    row = cur.fetchone()
    if row:
        return str(row[0])

    cur.execute(
        """
        INSERT INTO businesses (dedup_key, name, address, city, state, website, phone, category)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (dedup_key, name, address or None, city or None, state or None,
         website or None, phone or None, category or None),
    )
    return str(cur.fetchone()[0])


# ----------------------------------------------------------------------------
# mirror handlers — one per SQLite write op
# ----------------------------------------------------------------------------
def _do_mirror_contact(conn, payload: dict) -> str:
    """Insert a contact into business_intel.contacts; returns the new UUID."""
    with conn.cursor() as cur:
        biz_id = None
        if payload.get("company"):
            biz_id = _upsert_business(
                cur,
                name=payload["company"],
                address=payload.get("address", ""),
                website=payload.get("website", ""),
                phone=payload.get("phone", ""),
            )
        cur.execute(
            """
            INSERT INTO contacts (business_id, name, title, company, email, phone, phone2,
                                  website, address, location, source, tags, notes,
                                  created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (biz_id, payload["name"], payload.get("title"), payload.get("company"),
             payload.get("email"), payload.get("phone"), payload.get("phone2"),
             payload.get("website"), payload.get("address"), payload.get("location"),
             payload.get("source"), _parse_tags(payload.get("tags", "[]")),
             payload.get("notes"),
             _parse_ts(payload.get("created_at")),
             _parse_ts(payload.get("updated_at"))),
        )
        return str(cur.fetchone()[0])


def _do_mirror_audit(conn, payload: dict) -> str:
    """Insert an audit into business_intel.audits; returns the new UUID.

    If the audit is linked to a Jennifer contact, the contact's pg_id is
    passed via `contact_pg_id` in the payload (set by contacts_db._mirror).
    We resolve the audit's business_id from that contact's business_id
    rather than re-creating a business from the company name, which would
    duplicate businesses on every audit.
    """
    with conn.cursor() as cur:
        biz_id = None
        contact_pg = payload.get("contact_pg_id")
        if contact_pg:
            cur.execute("SELECT business_id FROM contacts WHERE id = %s", (contact_pg,))
            r = cur.fetchone()
            if r and r[0]:
                biz_id = r[0]
        if biz_id is None and payload.get("company"):
            biz_id = _upsert_business(cur, name=payload["company"])
        cur.execute(
            """
            INSERT INTO audits (business_id, audit_type, audit_status, score, summary,
                                details, ran_at, ran_by, legacy_key)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            RETURNING id
            """,
            (biz_id, payload["audit_type"], "complete",
             payload.get("google_rating"),
             payload.get("summary"),
             payload.get("full_data", "{}"),
             _parse_ts(payload.get("created_at")),
             f"jennifer:{payload.get('model_used', '')}",
             f"jcontact:{payload.get('contact_id')}" if payload.get("contact_id") else None),
        )
        return str(cur.fetchone()[0])


def _do_mirror_research(conn, payload: dict) -> str:
    with conn.cursor() as cur:
        biz_id = None
        if payload.get("subject"):
            biz_id = _upsert_business(cur, name=payload["subject"])
        sources = _parse_tags(payload.get("sources", "[]"))
        cur.execute(
            """
            INSERT INTO research (business_id, topic, body, sources, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (biz_id, payload["subject"], payload.get("findings", ""),
             sources, _parse_ts(payload.get("created_at"))),
        )
        return str(cur.fetchone()[0])


def _do_mirror_interaction(conn, payload: dict) -> str:
    with conn.cursor() as cur:
        # Prefer the contact's pg_id passed explicitly by the caller —
        # contacts_db.py looks it up from SQLite contacts.pg_id, which is the
        # canonical mapping for Jennifer-managed contacts.
        contact_uuid = payload.get("contact_pg_id")

        # Fallback: source_refs scoped to the JENNIFER source only.
        # The unscoped lookup is unsafe because integer ids collide across
        # sources (Jennifer contact 44 ≠ crm_db_rds contact 44, but their
        # source_refs.source_row_id values are both '44').
        if contact_uuid is None and payload.get("contact_id"):
            cur.execute(
                """
                SELECT sr.target_id
                FROM source_refs sr
                JOIN sources s ON s.id = sr.source_id
                WHERE sr.source_table = 'contacts'
                  AND sr.source_row_id = %s
                  AND sr.target_table = 'contacts'
                  AND s.source_name = 'jennifer'
                ORDER BY sr.id DESC LIMIT 1
                """,
                (str(payload["contact_id"]),),
            )
            row = cur.fetchone()
            if row:
                contact_uuid = row[0]

        cur.execute(
            """
            INSERT INTO interactions (contact_id, interaction_type, subject, body, happened_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (contact_uuid, payload["interaction_type"],
             payload.get("summary"), payload.get("details"),
             _parse_ts(payload.get("created_at"))),
        )
        return str(cur.fetchone()[0])


_OP_HANDLERS = {
    "contact": _do_mirror_contact,
    "audit": _do_mirror_audit,
    "research": _do_mirror_research,
    "interaction": _do_mirror_interaction,
}


# ----------------------------------------------------------------------------
# public mirror API — called from contacts_db.py after each successful write
# ----------------------------------------------------------------------------
def mirror(op: str, sqlite_id: int, payload: dict) -> Optional[str]:
    """Mirror a write to Postgres.

    Returns the Postgres UUID string on success, or None on failure (in which
    case the op is queued to the replay journal).
    """
    if not MIRROR_ENABLED:
        return None
    handler = _OP_HANDLERS.get(op)
    if handler is None:
        logger.warning("pg_mirror: unknown op %s", op)
        return None

    conn = _get_conn()
    if conn is None:
        _append_replay(op, payload, sqlite_id, "postgres unavailable")
        return None

    try:
        result = handler(conn, payload)
        conn.commit()
        return result
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("pg_mirror: mirror %s failed: %s", op, e)
        _append_replay(op, payload, sqlite_id, str(e))
        return None


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Jennifer Postgres mirror utility")
    ap.add_argument("--drain", action="store_true", help="drain the replay journal")
    ap.add_argument("--status", action="store_true", help="show mirror status")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.status or not (args.drain):
        conn = _get_conn()
        pending = 0
        if REPLAY_JOURNAL.exists():
            pending = sum(1 for _ in REPLAY_JOURNAL.open())
        print(f"psycopg installed:    {HAVE_PSYCOPG}")
        print(f"mirror enabled:       {MIRROR_ENABLED}")
        print(f"postgres reachable:   {conn is not None}")
        print(f"dsn:                  {PG_DSN}")
        print(f"replay journal:       {REPLAY_JOURNAL}")
        print(f"pending replay ops:   {pending}")

    if args.drain:
        drained, pending = replay_pending()
        print(f"drained={drained} still_pending={pending}")


if __name__ == "__main__":
    main()
