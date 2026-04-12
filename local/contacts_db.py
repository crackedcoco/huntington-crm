"""Contacts & Audit Database — SQLite-backed CRM for storing business audits, contacts, and research.

SQLite is the authoritative write path (fast, offline-safe). Every successful
write is mirrored to the unified `business_intel` Postgres database via
`pg_mirror` — if Postgres is unreachable, the mirror op is queued to a replay
journal and drained on the next successful connection. Read paths stay on SQLite.

See `pg_mirror.py` for the mirror implementation.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from . import pg_mirror
except ImportError:
    pg_mirror = None  # type: ignore

DB_DIR = Path.home() / ".jennifer"
DB_PATH = DB_DIR / "contacts.db"


def _lookup_contact_pg_id(contact_id: int) -> str | None:
    """Read the Postgres UUID stored in contacts.pg_id for a SQLite contact id.

    This is the canonical way to map a Jennifer contact integer id → its
    business_intel UUID. Used to attach interactions and audits to the
    correct Postgres contact row without consulting source_refs (which
    is shared across multiple sources and prone to integer-id collisions).
    """
    if contact_id is None:
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        row = conn.execute("SELECT pg_id FROM contacts WHERE id = ?", (contact_id,)).fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _mirror(op: str, sqlite_id: int, payload: dict) -> None:
    """Best-effort Postgres mirror. Never raises — the SQLite write is source of truth.

    For interactions and audits with a contact_id, the contact's pg_id is
    looked up from SQLite and added to the payload as `contact_pg_id` so
    pg_mirror can attach the row to the correct contact directly without
    a source_refs lookup (which is shared across multiple sources and would
    otherwise pick the wrong row when integer ids collide across sources).
    """
    if pg_mirror is None:
        return
    try:
        # Resolve the contact's pg_id ahead of time for ops that link to a contact
        if op in ("interaction", "audit") and payload.get("contact_id"):
            pg_id = _lookup_contact_pg_id(payload["contact_id"])
            if pg_id:
                payload = {**payload, "contact_pg_id": pg_id}

        pg_uuid = pg_mirror.mirror(op, sqlite_id, payload)
        if pg_uuid:
            table = {"contact": "contacts", "audit": "audits",
                     "research": "research", "interaction": "interactions"}.get(op)
            if table:
                try:
                    conn = sqlite3.connect(str(DB_PATH))
                    conn.execute(f"UPDATE {table} SET pg_id = ? WHERE id = ?", (pg_uuid, sqlite_id))
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
    except Exception:
        # Never let mirror failures break Jennifer's write path
        pass


def get_db() -> sqlite3.Connection:
    """Get database connection, creating tables if needed."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _init_tables(conn)
    return conn


def _init_tables(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist, and add pg_id columns for mirror linkage."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            title TEXT DEFAULT '',
            company TEXT DEFAULT '',
            email TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            phone2 TEXT DEFAULT '',
            website TEXT DEFAULT '',
            address TEXT DEFAULT '',
            location TEXT DEFAULT '',
            source TEXT DEFAULT '',
            tags TEXT DEFAULT '[]',
            notes TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER,
            company TEXT NOT NULL,
            audit_type TEXT NOT NULL,
            google_rating REAL,
            google_reviews INTEGER,
            summary TEXT DEFAULT '',
            full_data TEXT DEFAULT '{}',
            model_used TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        );

        CREATE TABLE IF NOT EXISTS research (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER,
            subject TEXT NOT NULL,
            research_type TEXT DEFAULT 'profile',
            findings TEXT DEFAULT '',
            sources TEXT DEFAULT '[]',
            created_at TEXT NOT NULL,
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        );

        CREATE TABLE IF NOT EXISTS interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL,
            interaction_type TEXT NOT NULL,
            summary TEXT DEFAULT '',
            details TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        );

        CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts(company);
        CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name);
        CREATE INDEX IF NOT EXISTS idx_contacts_location ON contacts(location);
        CREATE INDEX IF NOT EXISTS idx_audits_company ON audits(company);
        CREATE INDEX IF NOT EXISTS idx_audits_type ON audits(audit_type);
    """)
    # Add pg_id columns if they don't exist — for linking rows to their Postgres mirror
    for table in ("contacts", "audits", "research", "interactions"):
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if "pg_id" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN pg_id TEXT")
    conn.commit()


# ─── CONTACTS ───

def add_contact(
    name: str,
    company: str = "",
    title: str = "",
    email: str = "",
    phone: str = "",
    phone2: str = "",
    website: str = "",
    address: str = "",
    location: str = "",
    source: str = "",
    tags: list[str] | None = None,
    notes: str = "",
) -> int:
    """Add a new contact. Returns the contact ID. Mirrors to Postgres."""
    now = datetime.utcnow().isoformat()
    tags_json = json.dumps(tags or [])
    conn = get_db()
    cursor = conn.execute(
        """INSERT INTO contacts (name, title, company, email, phone, phone2, website, address, location, source, tags, notes, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (name, title, company, email, phone, phone2, website, address, location, source, tags_json, notes, now, now),
    )
    conn.commit()
    contact_id = cursor.lastrowid
    conn.close()
    _mirror("contact", contact_id, {
        "name": name, "title": title, "company": company, "email": email,
        "phone": phone, "phone2": phone2, "website": website, "address": address,
        "location": location, "source": source, "tags": tags_json, "notes": notes,
        "created_at": now, "updated_at": now,
    })
    return contact_id


def find_contact(name: str = "", company: str = "", email: str = "") -> list[dict]:
    """Search contacts by name, company, or email."""
    conn = get_db()
    conditions = []
    params = []
    if name:
        conditions.append("name LIKE ?")
        params.append(f"%{name}%")
    if company:
        conditions.append("company LIKE ?")
        params.append(f"%{company}%")
    if email:
        conditions.append("email LIKE ?")
        params.append(f"%{email}%")

    where = " AND ".join(conditions) if conditions else "1=1"
    rows = conn.execute(f"SELECT * FROM contacts WHERE {where} ORDER BY updated_at DESC", params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_contact(contact_id: int) -> dict | None:
    """Get a contact by ID."""
    conn = get_db()
    row = conn.execute("SELECT * FROM contacts WHERE id = ?", (contact_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_contact(contact_id: int, **kwargs) -> None:
    """Update contact fields."""
    conn = get_db()
    kwargs["updated_at"] = datetime.utcnow().isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE contacts SET {sets} WHERE id = ?", (*kwargs.values(), contact_id))
    conn.commit()
    conn.close()


def list_contacts(limit: int = 50, offset: int = 0, location: str = "", tag: str = "") -> list[dict]:
    """List contacts with optional filters."""
    conn = get_db()
    conditions = []
    params = []
    if location:
        conditions.append("location LIKE ?")
        params.append(f"%{location}%")
    if tag:
        conditions.append("tags LIKE ?")
        params.append(f"%{tag}%")
    where = " AND ".join(conditions) if conditions else "1=1"
    rows = conn.execute(
        f"SELECT * FROM contacts WHERE {where} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        (*params, limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── AUDITS ───

def add_audit(
    company: str,
    audit_type: str,
    google_rating: float | None = None,
    google_reviews: int | None = None,
    summary: str = "",
    full_data: dict | None = None,
    model_used: str = "",
    contact_id: int | None = None,
) -> int:
    """Store an audit result and auto-log an interaction if contact_id is provided.

    Returns audit ID. Mirrors both the audit and (if applicable) the interaction to Postgres.
    """
    now = datetime.utcnow().isoformat()
    full_data_json = json.dumps(full_data or {})
    conn = get_db()
    cursor = conn.execute(
        """INSERT INTO audits (contact_id, company, audit_type, google_rating, google_reviews, summary, full_data, model_used, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (contact_id, company, audit_type, google_rating, google_reviews, summary, full_data_json, model_used, now),
    )
    conn.commit()
    audit_id = cursor.lastrowid

    interaction_id = None
    interaction_summary = ""
    interaction_details = ""
    if contact_id:
        interaction_summary = f"{audit_type} audit — {summary[:120]}"
        interaction_details = f"Audit #{audit_id} for {company}. Type: {audit_type}."
        icur = conn.execute(
            """INSERT INTO interactions (contact_id, interaction_type, summary, details, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (contact_id, "audit", interaction_summary, interaction_details, now),
        )
        conn.commit()
        interaction_id = icur.lastrowid

    conn.close()

    _mirror("audit", audit_id, {
        "company": company, "audit_type": audit_type,
        "google_rating": google_rating, "google_reviews": google_reviews,
        "summary": summary, "full_data": full_data_json,
        "model_used": model_used, "contact_id": contact_id, "created_at": now,
    })
    if interaction_id:
        _mirror("interaction", interaction_id, {
            "contact_id": contact_id, "interaction_type": "audit",
            "summary": interaction_summary, "details": interaction_details,
            "created_at": now,
        })
    return audit_id


def get_audits(company: str = "", audit_type: str = "", limit: int = 20) -> list[dict]:
    """Get audits filtered by company or type."""
    conn = get_db()
    conditions = []
    params = []
    if company:
        conditions.append("company LIKE ?")
        params.append(f"%{company}%")
    if audit_type:
        conditions.append("audit_type = ?")
        params.append(audit_type)
    where = " AND ".join(conditions) if conditions else "1=1"
    rows = conn.execute(
        f"SELECT * FROM audits WHERE {where} ORDER BY created_at DESC LIMIT ?",
        (*params, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── RESEARCH ───

def add_research(
    subject: str,
    findings: str,
    research_type: str = "profile",
    sources: list[str] | None = None,
    contact_id: int | None = None,
) -> int:
    """Store research findings. Returns research ID. Mirrors to Postgres."""
    now = datetime.utcnow().isoformat()
    sources_json = json.dumps(sources or [])
    conn = get_db()
    cursor = conn.execute(
        """INSERT INTO research (contact_id, subject, research_type, findings, sources, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (contact_id, subject, research_type, findings, sources_json, now),
    )
    conn.commit()
    research_id = cursor.lastrowid
    conn.close()
    _mirror("research", research_id, {
        "contact_id": contact_id, "subject": subject,
        "research_type": research_type, "findings": findings,
        "sources": sources_json, "created_at": now,
    })
    return research_id


def get_research(subject: str = "", limit: int = 20) -> list[dict]:
    """Get research by subject."""
    conn = get_db()
    if subject:
        rows = conn.execute(
            "SELECT * FROM research WHERE subject LIKE ? ORDER BY created_at DESC LIMIT ?",
            (f"%{subject}%", limit),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM research ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── INTERACTIONS ───

def add_interaction(
    contact_id: int,
    interaction_type: str,
    summary: str = "",
    details: str = "",
) -> int:
    """Log an interaction with a contact. Mirrors to Postgres."""
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cursor = conn.execute(
        """INSERT INTO interactions (contact_id, interaction_type, summary, details, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (contact_id, interaction_type, summary, details, now),
    )
    conn.commit()
    interaction_id = cursor.lastrowid
    conn.close()
    _mirror("interaction", interaction_id, {
        "contact_id": contact_id, "interaction_type": interaction_type,
        "summary": summary, "details": details, "created_at": now,
    })
    return interaction_id


# ─── MUTUAL CONNECTIONS ───

def find_mutual_connections(company: str = "", tags: list[str] | None = None, event: str = "", exclude_id: int | None = None) -> list[dict]:
    """Find contacts who share a company, tags, or event with a given contact.

    Useful for discovering mutual connections when scanning a new business card.
    """
    conn = get_db()
    results = []

    # By company (fuzzy)
    if company:
        rows = conn.execute(
            "SELECT * FROM contacts WHERE company LIKE ? AND id != ?",
            (f"%{company}%", exclude_id or 0),
        ).fetchall()
        for r in rows:
            d = dict(r)
            d["match_reason"] = f"same company: {company}"
            results.append(d)

    # By tags
    if tags:
        all_contacts = conn.execute(
            "SELECT * FROM contacts WHERE id != ?", (exclude_id or 0,)
        ).fetchall()
        for r in all_contacts:
            contact_tags = json.loads(r["tags"]) if r["tags"] else []
            shared = set(tags) & set(contact_tags)
            if shared:
                d = dict(r)
                d["match_reason"] = f"shared tags: {', '.join(shared)}"
                results.append(d)

    # By event (search interactions)
    if event:
        rows = conn.execute(
            """SELECT DISTINCT c.* FROM contacts c
               JOIN interactions i ON i.contact_id = c.id
               WHERE i.summary LIKE ? AND c.id != ?""",
            (f"%{event}%", exclude_id or 0),
        ).fetchall()
        for r in rows:
            d = dict(r)
            d["match_reason"] = f"same event: {event}"
            # Avoid duplicates
            if not any(x["id"] == d["id"] for x in results):
                results.append(d)

    conn.close()
    return results


# ─── STATS ───

def get_stats() -> dict:
    """Get database statistics."""
    conn = get_db()
    contacts = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    audits = conn.execute("SELECT COUNT(*) FROM audits").fetchone()[0]
    research = conn.execute("SELECT COUNT(*) FROM research").fetchone()[0]
    interactions = conn.execute("SELECT COUNT(*) FROM interactions").fetchone()[0]
    locations = conn.execute("SELECT DISTINCT location FROM contacts WHERE location != ''").fetchall()
    conn.close()
    return {
        "contacts": contacts,
        "audits": audits,
        "research": research,
        "interactions": interactions,
        "locations": [r[0] for r in locations],
    }


# ═══════════════════════════════════════════════════════════════════════════
# UNIFIED HELPERS — read from business_intel Postgres
# ═══════════════════════════════════════════════════════════════════════════
# These opt-in helpers query the unified business_intel Postgres database
# instead of Jennifer's local SQLite. They see the full 165-contact universe
# across Jennifer + crm-db RDS + every other ingested source, plus the 71K
# scraped businesses and 800K reviews.
#
# The SQLite-backed functions above stay untouched — this is purely additive.
# If Postgres is unavailable, unified helpers fall back to their SQLite
# counterparts so Jennifer degrades gracefully.
# ═══════════════════════════════════════════════════════════════════════════

import os as _os

try:
    import psycopg as _psycopg
    _HAVE_PSYCOPG = True
except ImportError:
    _HAVE_PSYCOPG = False
    _psycopg = None  # type: ignore

_BUSINESS_INTEL_DSN = _os.environ.get("BUSINESS_INTEL_DSN", "postgresql:///business_intel")
_pg_conn_cache = None


def _pg() -> "_psycopg.Connection | None":
    """Lazy connection to business_intel. Returns None if unavailable."""
    global _pg_conn_cache
    if not _HAVE_PSYCOPG:
        return None
    if _pg_conn_cache is not None:
        try:
            _pg_conn_cache.execute("SELECT 1")
            return _pg_conn_cache
        except Exception:
            try:
                _pg_conn_cache.close()
            except Exception:
                pass
            _pg_conn_cache = None
    try:
        _pg_conn_cache = _psycopg.connect(_BUSINESS_INTEL_DSN, connect_timeout=2)
        return _pg_conn_cache
    except Exception:
        return None


def _row_to_dict(row, cols) -> dict:
    return {col: (str(val) if hasattr(val, "hex") else val) for col, val in zip(cols, row)}


def find_contact_unified(name: str = "", company: str = "", email: str = "",
                         limit: int = 50) -> list[dict]:
    """Search ALL contacts in business_intel — Jennifer + crm-db + everything.

    Falls back to SQLite find_contact() if Postgres is unavailable.
    """
    conn = _pg()
    if conn is None:
        return find_contact(name=name, company=company, email=email)

    conditions = []
    params: list = []
    if name:
        conditions.append("lower(c.name) LIKE lower(%s)")
        params.append(f"%{name}%")
    if company:
        conditions.append("lower(c.company) LIKE lower(%s)")
        params.append(f"%{company}%")
    if email:
        conditions.append("lower(c.email) LIKE lower(%s)")
        params.append(f"%{email}%")

    where = " AND ".join(conditions) if conditions else "TRUE"
    sql = f"""
        SELECT c.id, c.name, c.title, c.company, c.email, c.phone, c.phone2,
               c.website, c.address, c.location, c.source, c.tags, c.notes,
               c.created_at, c.updated_at,
               c.business_id,
               b.rating_avg AS business_rating,
               b.review_count AS business_review_count,
               b.category AS business_category
        FROM contacts c
        LEFT JOIN businesses b ON b.id = c.business_id
        WHERE {where}
        ORDER BY c.updated_at DESC NULLS LAST
        LIMIT {int(limit)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [_row_to_dict(r, cols) for r in cur.fetchall()]


def list_contacts_unified(limit: int = 50, offset: int = 0, location: str = "",
                          tag: str = "", source: str = "") -> list[dict]:
    """List contacts from business_intel with optional filters.

    source='crm_db_rds' → only contacts from the web CRM
    source='jennifer'   → only contacts that Jennifer originally added
    source=''           → all sources
    """
    conn = _pg()
    if conn is None:
        return list_contacts(limit=limit, offset=offset, location=location, tag=tag)

    conditions = []
    params: list = []
    if location:
        conditions.append("lower(location) LIKE lower(%s)")
        params.append(f"%{location}%")
    if tag:
        conditions.append("%s = ANY(tags)")
        params.append(tag)
    if source:
        conditions.append("source = %s")
        params.append(source)
    where = " AND ".join(conditions) if conditions else "TRUE"
    sql = f"""
        SELECT id, name, title, company, email, phone, location, source, tags,
               created_at, updated_at, business_id
        FROM contacts
        WHERE {where}
        ORDER BY updated_at DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params + [limit, offset])
        cols = [d[0] for d in cur.description]
        return [_row_to_dict(r, cols) for r in cur.fetchall()]


def get_contact_unified(contact_id: str) -> dict | None:
    """Fetch a single contact from business_intel by UUID.

    Accepts either a Postgres UUID string or a Jennifer SQLite integer id
    (in which case it looks up the pg_id mapping).
    """
    conn = _pg()
    if conn is None:
        try:
            return get_contact(int(contact_id))
        except (ValueError, TypeError):
            return None

    # If it looks like an integer, resolve via Jennifer SQLite → pg_id
    uuid_str = str(contact_id)
    if uuid_str.isdigit():
        sq = get_db()
        row = sq.execute("SELECT pg_id FROM contacts WHERE id = ?", (int(uuid_str),)).fetchone()
        sq.close()
        if not row or not row["pg_id"]:
            return None
        uuid_str = row["pg_id"]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.*, b.name AS business_name, b.rating_avg, b.review_count,
                   b.category AS business_category, b.website AS business_website
            FROM contacts c
            LEFT JOIN businesses b ON b.id = c.business_id
            WHERE c.id = %s
            """,
            (uuid_str,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return _row_to_dict(row, cols)


def get_stats_unified() -> dict:
    """Unified stats across the whole business_intel database.

    Returns counts across contacts, interactions, audits, research,
    businesses, reviews, business_health, plus a source breakdown.
    """
    conn = _pg()
    if conn is None:
        return get_stats()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM contacts) AS contacts,
              (SELECT COUNT(*) FROM interactions) AS interactions,
              (SELECT COUNT(*) FROM audits) AS audits,
              (SELECT COUNT(*) FROM research) AS research,
              (SELECT COUNT(*) FROM businesses) AS businesses,
              (SELECT COUNT(*) FROM reviews) AS reviews,
              (SELECT COUNT(*) FROM business_health) AS business_health
        """)
        totals = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        cur.execute(
            "SELECT source, COUNT(*) FROM contacts GROUP BY source ORDER BY COUNT(*) DESC"
        )
        by_source = {(r[0] or "(none)"): r[1] for r in cur.fetchall()}
    return {**totals, "contacts_by_source": by_source}


def search_businesses_unified(query: str, limit: int = 20,
                              city: str = "", state: str = "") -> list[dict]:
    """Search the 71K business universe by fuzzy name match.

    This is NOT the contacts table — it's the raw businesses ingested from
    the six scrape sources (wv_reviews, merged_businesses, wv_scrape, etc.)
    plus every business Jennifer has seen. Use for prospecting.
    """
    conn = _pg()
    if conn is None:
        return []
    conditions = ["name ILIKE %s"]
    params: list = [f"%{query}%"]
    if city:
        conditions.append("lower(city) = lower(%s)")
        params.append(city)
    if state:
        conditions.append("lower(state) = lower(%s)")
        params.append(state)
    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, name, address, city, state, phone, website, category,
               rating_avg, review_count, source_count, last_scraped
        FROM businesses
        WHERE {where}
        ORDER BY review_count DESC NULLS LAST
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params + [limit])
        cols = [d[0] for d in cur.description]
        return [_row_to_dict(r, cols) for r in cur.fetchall()]


def top_prospects_unified(city: str = "", min_score: int = 80,
                          limit: int = 20, uncontacted: bool = True) -> list[dict]:
    """Return high-value prospects from business_health, optionally filtered
    to businesses without an existing contact.

    This is the killer query the morning briefing unlocks: 'who should I be
    reaching out to in Huntington that I'm not already talking to?'
    """
    conn = _pg()
    if conn is None:
        return []

    uncontacted_clause = """
        AND NOT EXISTS (
          SELECT 1 FROM contacts c WHERE c.business_id = b.id
        )
    """ if uncontacted else ""

    city_clause = "AND lower(b.city) = lower(%s)" if city else ""

    sql = f"""
        SELECT
          b.id, b.name, b.city, b.state, b.website, b.rating_avg, b.review_count,
          h.health_score,
          (h.category_scores->>'prospect_score')::float AS prospect_score,
          h.category_scores->'top_recommendations' AS recommendations
        FROM businesses b
        JOIN LATERAL (
          SELECT * FROM business_health WHERE business_id = b.id
          ORDER BY computed_at DESC NULLS LAST LIMIT 1
        ) h ON TRUE
        WHERE (h.category_scores->>'prospect_score')::float >= %s
          {city_clause}
          {uncontacted_clause}
        ORDER BY (h.category_scores->>'prospect_score')::float DESC
        LIMIT %s
    """
    params = [min_score]
    if city:
        params.append(city)
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [_row_to_dict(r, cols) for r in cur.fetchall()]


def get_contact_full_dossier(contact_id: str) -> dict | None:
    """Everything we know about a contact — merged from contacts + business +
    reviews + audits + interactions + research + business_health.

    The ultimate 'tell me about this person/business' query.
    """
    conn = _pg()
    if conn is None:
        return None
    contact = get_contact_unified(contact_id)
    if not contact:
        return None
    biz_id = contact.get("business_id")
    with conn.cursor() as cur:
        # Interactions
        cur.execute(
            "SELECT id, interaction_type, subject, body, happened_at, direction "
            "FROM interactions WHERE contact_id = %s ORDER BY happened_at DESC NULLS LAST LIMIT 20",
            (contact["id"],),
        )
        cols = [d[0] for d in cur.description]
        interactions = [_row_to_dict(r, cols) for r in cur.fetchall()]

        # Audits (business-level)
        audits = []
        if biz_id:
            cur.execute(
                "SELECT id, audit_type, audit_status, score, summary, ran_at "
                "FROM audits WHERE business_id = %s ORDER BY ran_at DESC NULLS LAST LIMIT 10",
                (biz_id,),
            )
            cols = [d[0] for d in cur.description]
            audits = [_row_to_dict(r, cols) for r in cur.fetchall()]

        # Recent reviews
        reviews = []
        if biz_id:
            cur.execute(
                "SELECT rating, text, author_handle, reviewed_at "
                "FROM reviews WHERE business_id = %s "
                "ORDER BY reviewed_at DESC NULLS LAST LIMIT 5",
                (biz_id,),
            )
            cols = [d[0] for d in cur.description]
            reviews = [_row_to_dict(r, cols) for r in cur.fetchall()]

        # Research
        research = []
        if biz_id:
            cur.execute(
                "SELECT topic, body, sources, created_at "
                "FROM research WHERE business_id = %s ORDER BY created_at DESC LIMIT 5",
                (biz_id,),
            )
            cols = [d[0] for d in cur.description]
            research = [_row_to_dict(r, cols) for r in cur.fetchall()]

        # Business health
        health = None
        if biz_id:
            cur.execute(
                "SELECT health_score, category_scores, computed_at "
                "FROM business_health WHERE business_id = %s "
                "ORDER BY computed_at DESC NULLS LAST LIMIT 1",
                (biz_id,),
            )
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                health = _row_to_dict(row, cols)

    return {
        "contact": contact,
        "interactions": interactions,
        "audits": audits,
        "recent_reviews": reviews,
        "research": research,
        "business_health": health,
    }
