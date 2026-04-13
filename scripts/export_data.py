#!/usr/bin/env python3
"""
Export all CRM data from Postgres (business_intel) to dashboard/data.json.
Run from anywhere -- paths are resolved absolutely.

Previously read from SQLite (~/.jennifer/contacts.db); rewired 2026-04-13
to use Postgres as the single source of truth (169 contacts vs SQLite's 40).
"""

import json
import os
from datetime import datetime, timezone

import psycopg

# --- Path resolution -----------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(REPO_ROOT, "dashboard", "data.json")
DSN = os.environ.get("BUSINESS_INTEL_DSN", "postgresql:///business_intel")

# --- Helpers ------------------------------------------------------------------


def fetch_table(conn, name: str) -> list[dict]:
    """Return all rows from *name* as a list of dicts, or [] if table is absent."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=%s)",
            (name,),
        )
        if not cur.fetchone()[0]:
            print(f"  WARNING: table '{name}' not found -- skipping")
            return []
        cur.execute(f"SELECT * FROM {name}")
        cols = [d[0] for d in cur.description]
        return [{col: (str(val) if hasattr(val, "hex") else val) for col, val in zip(cols, row)}
                for row in cur.fetchall()]


# --- Main ---------------------------------------------------------------------

def main() -> None:
    conn = psycopg.connect(DSN)

    print(f"Reading from : {DSN}")
    print(f"Writing to   : {OUTPUT_PATH}")
    print()

    contacts     = fetch_table(conn, "contacts")
    audits       = fetch_table(conn, "audits")
    research     = fetch_table(conn, "research")
    interactions = fetch_table(conn, "interactions")

    # Enrich audits with business names from the businesses table
    biz_names = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='businesses')"
        )
        if cur.fetchone()[0]:
            cur.execute("SELECT id, name FROM businesses")
            biz_names = {str(row[0]): row[1] for row in cur.fetchall()}
            print(f"  Loaded {len(biz_names):,} business names for audit enrichment")

    for a in audits:
        bid = a.get("business_id")
        if bid and str(bid) in biz_names and not a.get("company"):
            a["company"] = biz_names[str(bid)]

    for r in research:
        bid = r.get("business_id")
        if bid and str(bid) in biz_names and not r.get("company"):
            r["company"] = biz_names[str(bid)]

    conn.close()

    # Unique, sorted locations from contacts (skip blanks)
    locations = sorted(
        {c["location"] for c in contacts if c.get("location")}
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats": {
            "contacts":     len(contacts),
            "audits":       len(audits),
            "research":     len(research),
            "interactions": len(interactions),
            "locations":    locations,
        },
        "contacts":     contacts,
        "audits":       audits,
        "research":     research,
        "interactions": interactions,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)

    print("Export complete:")
    print(f"  contacts     : {len(contacts):,}")
    print(f"  audits       : {len(audits):,}")
    print(f"  research     : {len(research):,}")
    print(f"  interactions : {len(interactions):,}")
    print(f"  locations    : {len(locations):,} unique")
    print(f"  output       : {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
