"""CRM Lambda handler — contacts, audits, and interactions API.

Routes:
  GET    /api/contacts                  list_contacts
  POST   /api/contacts                  create_contact
  GET    /api/contacts/{id}             get_contact
  PUT    /api/contacts/{id}             update_contact
  DELETE /api/contacts/{id}             delete_contact
  POST   /api/contacts/{id}/audits      create_audit
  GET    /api/audits                    list_audits
  POST   /api/contacts/{id}/interactions create_interaction
  GET    /api/interactions              list_interactions

Plan limits enforced:
  starter  — 100 contacts max
  growth   — 500 contacts max
  scale    — unlimited
"""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import UTC, datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key

from shared.auth import require_tenant
from shared.db import TenantDB
from shared.features import check_feature, check_limit
from shared.rate_limit import check_rate_limit
from shared.response import (
    created,
    error,
    forbidden,
    not_found,
    options_preflight,
    server_error,
    success,
)

TABLE_NAME = os.environ["TABLE_NAME"]


# ── Routing ───────────────────────────────────────────────────────────────────

def handler(event: dict, context: object) -> dict:
    """Lambda entry point."""
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    if method == "OPTIONS":
        return options_preflight()

    try:
        tenant = require_tenant(event)
    except ValueError as exc:
        return error(str(exc), 401)

    db = TenantDB(TABLE_NAME, tenant["tenant_id"])

    # Per-tenant rate limit check (100 requests/min)
    if not check_rate_limit(TABLE_NAME, tenant["tenant_id"], max_requests=100):
        return error("Rate limit exceeded. Slow down.", 429)

    # /api/contacts
    if path == "/api/contacts":
        if method == "GET":
            return list_contacts(db, tenant, event)
        if method == "POST":
            return create_contact(db, tenant, event)

    # /api/contacts/{id}/audits
    m = re.match(r"^/api/contacts/([^/]+)/audits$", path)
    if m:
        contact_id = m.group(1)
        if method == "POST":
            return create_audit(db, tenant, contact_id, event)

    # /api/contacts/{id}/interactions
    m = re.match(r"^/api/contacts/([^/]+)/interactions$", path)
    if m:
        contact_id = m.group(1)
        if method == "POST":
            return create_interaction(db, tenant, contact_id, event)

    # /api/contacts/{id}
    m = re.match(r"^/api/contacts/([^/]+)$", path)
    if m:
        contact_id = m.group(1)
        if method == "GET":
            return get_contact(db, tenant, contact_id, event)
        if method == "PUT":
            return update_contact(db, tenant, contact_id, event)
        if method == "DELETE":
            return delete_contact(db, tenant, contact_id, event)

    # /api/audits
    if path == "/api/audits" and method == "GET":
        return list_audits(db, tenant, event)

    # /api/interactions
    if path == "/api/interactions" and method == "GET":
        return list_interactions(db, tenant, event)

    return error(f"No route: {method} {path}", 404)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _body(event: dict) -> dict:
    """Parse request body as JSON, returning empty dict on failure."""
    raw = event.get("body") or "{}"
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _count_contacts(db: TenantDB) -> int:
    """Count active (non-deleted) contacts for the tenant."""
    resp = db.table.query(
        KeyConditionExpression=(
            Key("PK").eq(db._pk("CONTACT")) & Key("SK").begins_with("CONTACT#")
        ),
        FilterExpression=Attr("deleted").ne(True),
        Select="COUNT",
    )
    return resp.get("Count", 0)


def _count_entity(db: TenantDB, entity: str, sk_prefix: str) -> int:
    """Generic count for any entity type."""
    resp = db.table.query(
        KeyConditionExpression=(
            Key("PK").eq(db._pk(entity)) & Key("SK").begins_with(sk_prefix)
        ),
        Select="COUNT",
    )
    return resp.get("Count", 0)


# ── Contacts ──────────────────────────────────────────────────────────────────

def list_contacts(db: TenantDB, tenant: dict, event: dict) -> dict:
    """GET /api/contacts — paginated, searchable."""
    params = event.get("queryStringParameters") or {}
    limit = min(int(params.get("limit", 50)), 200)
    search = (params.get("q") or "").strip().lower()
    last_key_raw = params.get("nextToken")

    kwargs: dict = {
        "KeyConditionExpression": (
            Key("PK").eq(db._pk("CONTACT")) & Key("SK").begins_with("CONTACT#")
        ),
        "FilterExpression": Attr("deleted").ne(True),
        "Limit": limit,
        "ScanIndexForward": False,
    }

    if last_key_raw:
        try:
            kwargs["ExclusiveStartKey"] = json.loads(last_key_raw)
        except (json.JSONDecodeError, TypeError):
            pass

    resp = db.table.query(**kwargs)
    items = resp.get("Items", [])

    # Client-side search filter (DynamoDB doesn't do LIKE on arbitrary fields)
    if search:
        items = [
            c for c in items
            if search in (c.get("name") or "").lower()
            or search in (c.get("company") or "").lower()
            or search in (c.get("email") or "").lower()
        ]

    next_token = None
    if "LastEvaluatedKey" in resp:
        next_token = json.dumps(resp["LastEvaluatedKey"])

    return success({
        "contacts": items,
        "count": len(items),
        "nextToken": next_token,
    })


def create_contact(db: TenantDB, tenant: dict, event: dict) -> dict:
    """POST /api/contacts — validate required fields, enforce plan limit."""
    plan = tenant["plan"]

    # Feature check
    feature = "contacts_full" if plan != "starter" else "contacts_basic"
    if not check_feature(plan, feature) and not check_feature(plan, "contacts_basic"):
        return forbidden("Contacts are not available on your plan.")

    # Limit check
    current_count = _count_contacts(db)
    if not check_limit(plan, "contacts", current_count):
        limit_map = {"starter": 100, "growth": 500}
        cap = limit_map.get(plan, 0)
        return forbidden(
            f"You've hit the {cap}-contact limit for the {plan.title()} plan. "
            "Upgrade to add more contacts."
        )

    body = _body(event)
    name = (body.get("name") or "").strip()
    if not name:
        return error("name is required")

    contact_id = str(uuid.uuid4())
    data = {
        "name": name,
        "company": body.get("company", ""),
        "title": body.get("title", ""),
        "email": body.get("email", ""),
        "phone": body.get("phone", ""),
        "phone2": body.get("phone2", ""),
        "website": body.get("website", ""),
        "address": body.get("address", ""),
        "location": body.get("location", ""),
        "source": body.get("source", ""),
        "tags": body.get("tags", []),
        "notes": body.get("notes", ""),
        "createdAt": _now(),
        "deleted": False,
    }

    item = db.put_contact(contact_id, data)
    return created({"contact": item})


def get_contact(db: TenantDB, tenant: dict, contact_id: str, event: dict) -> dict:
    """GET /api/contacts/{id} — return contact + linked audits + interactions."""
    contact = db.get_contact(contact_id)
    if not contact or contact.get("deleted"):
        return not_found("Contact")

    # Fetch linked audits
    audits_resp = db.table.query(
        KeyConditionExpression=(
            Key("PK").eq(db._pk("AUDIT")) & Key("SK").begins_with(f"AUDIT#CONTACT#{contact_id}#")
        ),
        ScanIndexForward=False,
        Limit=50,
    )
    audits = audits_resp.get("Items", [])

    # Fetch linked interactions
    interactions_resp = db.table.query(
        KeyConditionExpression=(
            Key("PK").eq(db._pk("INTERACTION"))
            & Key("SK").begins_with(f"INTERACTION#CONTACT#{contact_id}#")
        ),
        ScanIndexForward=False,
        Limit=100,
    )
    interactions = interactions_resp.get("Items", [])

    return success({
        "contact": contact,
        "audits": audits,
        "interactions": interactions,
    })


def update_contact(db: TenantDB, tenant: dict, contact_id: str, event: dict) -> dict:
    """PUT /api/contacts/{id} — partial update."""
    contact = db.get_contact(contact_id)
    if not contact or contact.get("deleted"):
        return not_found("Contact")

    body = _body(event)
    allowed = {"name", "company", "title", "email", "phone", "phone2",
               "website", "address", "location", "source", "tags", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed}

    if not updates:
        return error("No updatable fields provided")

    updates["updatedAt"] = _now()

    # Merge with existing data and upsert
    merged = {**contact, **updates}
    # Remove DynamoDB key fields before re-putting
    for key in ("PK", "SK", "GSI1PK", "GSI1SK"):
        merged.pop(key, None)

    item = db.put_contact(contact_id, merged)
    return success({"contact": item})


def delete_contact(db: TenantDB, tenant: dict, contact_id: str, event: dict) -> dict:
    """DELETE /api/contacts/{id} — soft delete."""
    contact = db.get_contact(contact_id)
    if not contact or contact.get("deleted"):
        return not_found("Contact")

    # Soft delete: set deleted flag
    for key in ("PK", "SK", "GSI1PK", "GSI1SK"):
        contact.pop(key, None)
    contact["deleted"] = True
    contact["deletedAt"] = _now()
    db.put_contact(contact_id, contact)

    return success({"deleted": True, "contactId": contact_id})


# ── Audits ────────────────────────────────────────────────────────────────────

def create_audit(db: TenantDB, tenant: dict, contact_id: str, event: dict) -> dict:
    """POST /api/contacts/{id}/audits — create audit, auto-log interaction."""
    if not check_feature(tenant["plan"], "health_index") and not check_feature(tenant["plan"], "scan_website"):
        return forbidden("Audits require Growth plan or higher.")

    contact = db.get_contact(contact_id)
    if not contact or contact.get("deleted"):
        return not_found("Contact")

    body = _body(event)
    audit_type = (body.get("audit_type") or "").strip()
    company = (body.get("company") or contact.get("company") or "").strip()
    if not audit_type:
        return error("audit_type is required")

    audit_id = str(uuid.uuid4())
    now = _now()

    audit_item = {
        "PK": db._pk("AUDIT"),
        "SK": f"AUDIT#CONTACT#{contact_id}#{now}#{audit_id}",
        "auditId": audit_id,
        "contactId": contact_id,
        "tenantId": db.tenant_id,
        "company": company,
        "auditType": audit_type,
        "googleRating": body.get("google_rating"),
        "googleReviews": body.get("google_reviews"),
        "summary": body.get("summary", ""),
        "fullData": body.get("full_data", {}),
        "modelUsed": body.get("model_used", ""),
        "createdAt": now,
    }
    db.table.put_item(Item=audit_item)

    # Auto-log interaction
    interaction_id = str(uuid.uuid4())
    summary_text = body.get("summary", "")[:120]
    interaction_item = {
        "PK": db._pk("INTERACTION"),
        "SK": f"INTERACTION#CONTACT#{contact_id}#{now}#{interaction_id}",
        "interactionId": interaction_id,
        "contactId": contact_id,
        "tenantId": db.tenant_id,
        "interactionType": "audit",
        "summary": f"{audit_type} audit — {summary_text}",
        "details": f"Audit {audit_id} for {company}. Type: {audit_type}.",
        "linkedAuditId": audit_id,
        "createdAt": now,
    }
    db.table.put_item(Item=interaction_item)

    return created({"audit": audit_item, "interaction": interaction_item})


def list_audits(db: TenantDB, tenant: dict, event: dict) -> dict:
    """GET /api/audits — list audits for tenant, filter by company/type."""
    params = event.get("queryStringParameters") or {}
    company_filter = (params.get("company") or "").strip().lower()
    type_filter = (params.get("audit_type") or "").strip().lower()
    limit = min(int(params.get("limit", 50)), 200)

    resp = db.table.query(
        KeyConditionExpression=(
            Key("PK").eq(db._pk("AUDIT")) & Key("SK").begins_with("AUDIT#")
        ),
        Limit=limit,
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])

    if company_filter:
        items = [a for a in items if company_filter in (a.get("company") or "").lower()]
    if type_filter:
        items = [a for a in items if (a.get("auditType") or "").lower() == type_filter]

    return success({"audits": items, "count": len(items)})


# ── Interactions ──────────────────────────────────────────────────────────────

def create_interaction(db: TenantDB, tenant: dict, contact_id: str, event: dict) -> dict:
    """POST /api/contacts/{id}/interactions — log interaction."""
    contact = db.get_contact(contact_id)
    if not contact or contact.get("deleted"):
        return not_found("Contact")

    body = _body(event)
    interaction_type = (body.get("interaction_type") or "").strip()
    if not interaction_type:
        return error("interaction_type is required")

    interaction_id = str(uuid.uuid4())
    now = _now()

    item = {
        "PK": db._pk("INTERACTION"),
        "SK": f"INTERACTION#CONTACT#{contact_id}#{now}#{interaction_id}",
        "interactionId": interaction_id,
        "contactId": contact_id,
        "tenantId": db.tenant_id,
        "interactionType": interaction_type,
        "summary": body.get("summary", ""),
        "details": body.get("details", ""),
        "createdAt": now,
    }
    db.table.put_item(Item=item)

    return created({"interaction": item})


def list_interactions(db: TenantDB, tenant: dict, event: dict) -> dict:
    """GET /api/interactions — list interactions, filter by contact_id/type."""
    params = event.get("queryStringParameters") or {}
    contact_id_filter = (params.get("contact_id") or "").strip()
    type_filter = (params.get("interaction_type") or "").strip().lower()
    limit = min(int(params.get("limit", 100)), 500)

    if contact_id_filter:
        resp = db.table.query(
            KeyConditionExpression=(
                Key("PK").eq(db._pk("INTERACTION"))
                & Key("SK").begins_with(f"INTERACTION#CONTACT#{contact_id_filter}#")
            ),
            Limit=limit,
            ScanIndexForward=False,
        )
    else:
        resp = db.table.query(
            KeyConditionExpression=(
                Key("PK").eq(db._pk("INTERACTION")) & Key("SK").begins_with("INTERACTION#")
            ),
            Limit=limit,
            ScanIndexForward=False,
        )

    items = resp.get("Items", [])

    if type_filter:
        items = [i for i in items if (i.get("interactionType") or "").lower() == type_filter]

    return success({"interactions": items, "count": len(items)})
