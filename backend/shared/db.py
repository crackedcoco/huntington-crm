"""TenantDB — DynamoDB single-table wrapper with automatic tenant isolation.

All keys are automatically prefixed with TENANT#{tenant_id} so data from
different tenants can never leak across queries.

Key schema (PK / SK):
  Contacts:      TENANT#{tid}#CONTACT / CONTACT#{contact_id}
  Conversations: TENANT#{tid}#CONV    / CONV#{conv_id}
  Messages:      TENANT#{tid}#CONV#{conv_id}#MSG / MSG#{iso_timestamp}
  Usage:         TENANT#{tid}#USAGE   / DATE#{yyyy-mm-dd}

GSI2 (time-based):
  GSI2PK = TENANT#{tid}#CONV
  GSI2SK = {createdAt ISO}  — enables pagination sorted by recency
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, date
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key


class TenantDB:
    """DynamoDB access layer scoped to a single tenant."""

    def __init__(self, table_name: str, tenant_id: str) -> None:
        self.table = boto3.resource("dynamodb").Table(table_name)
        self.tenant_id = tenant_id

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _pk(self, entity: str) -> str:
        """Primary partition key for a tenant entity class."""
        return f"TENANT#{self.tenant_id}#{entity}"

    def _now_iso(self) -> str:
        return datetime.now(UTC).isoformat()

    # ── Profile (per-tenant settings + onboarding state) ─────────────────────
    #
    # One record per tenant at PK=TENANT#{tid}#PROFILE, SK=PROFILE. Holds
    # small flags and settings that the frontend wants to read on every
    # authenticated page load — onboarding completion, preferred defaults,
    # etc. Kept intentionally thin: anything that grows should move to its
    # own entity class (like memories, contacts, conversations).
    #
    # Prior to 2026-04-11 onboarding state lived ONLY in the browser's
    # localStorage which was fragile across devices, browsers, clearing
    # site data, and some Safari/ITP eviction scenarios. Moving it here
    # makes it authoritative and survives the obvious "log in from my
    # laptop tomorrow" case.

    def get_profile(self) -> dict:
        """Return the tenant's profile record, or {} if none exists.

        Safe to call on every page load — one DynamoDB GetItem, no join.
        Returns a plain dict so callers can `.get("onboarded", False)`
        etc. without key-error handling.
        """
        try:
            resp = self.table.get_item(
                Key={"PK": self._pk("PROFILE"), "SK": "PROFILE"}
            )
        except Exception:
            return {}
        item = resp.get("Item") or {}
        # Strip internal DynamoDB keys so the dict is safe to return over
        # the wire.
        return {
            k: v
            for k, v in item.items()
            if k not in ("PK", "SK", "GSI1PK", "GSI1SK", "GSI2PK", "GSI2SK")
        }

    def set_profile_field(self, key: str, value) -> None:
        """Upsert a single field on the tenant profile record.

        Creates the profile row on first write. Uses DynamoDB's native
        SET so the rest of the fields (if any) are preserved.
        """
        self.table.update_item(
            Key={"PK": self._pk("PROFILE"), "SK": "PROFILE"},
            UpdateExpression=(
                "SET #k = :v, tenantId = :tid, updatedAt = :now"
            ),
            ExpressionAttributeNames={"#k": key},
            ExpressionAttributeValues={
                ":v": value,
                ":tid": self.tenant_id,
                ":now": self._now_iso(),
            },
        )

    # ── Contacts ──────────────────────────────────────────────────────────────

    def put_contact(self, contact_id: str, data: dict[str, Any]) -> dict:
        """Upsert a contact record.

        Args:
            contact_id: Unique identifier for the contact (caller-generated or UUID).
            data: Arbitrary contact fields (name, email, company, notes, …).

        Returns:
            The full item as stored in DynamoDB.
        """
        item = {
            "PK": self._pk("CONTACT"),
            "SK": f"CONTACT#{contact_id}",
            # GSI1 — admin: all contacts across tenants by email
            "GSI1PK": "CONTACTS",
            "GSI1SK": f"TENANT#{self.tenant_id}#CONTACT#{contact_id}",
            "contactId": contact_id,
            "tenantId": self.tenant_id,
            "updatedAt": self._now_iso(),
            **data,
        }
        if "createdAt" not in item:
            item["createdAt"] = item["updatedAt"]

        self.table.put_item(Item=item)
        return item

    def get_contact(self, contact_id: str) -> dict | None:
        """Fetch a single contact by ID. Returns None if not found."""
        resp = self.table.get_item(
            Key={
                "PK": self._pk("CONTACT"),
                "SK": f"CONTACT#{contact_id}",
            }
        )
        return resp.get("Item")

    def list_contacts(self, limit: int = 50) -> list[dict]:
        """List contacts for this tenant, newest first (by createdAt)."""
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("CONTACT"))
                & Key("SK").begins_with("CONTACT#")
            ),
            Limit=limit,
            ScanIndexForward=False,
        )
        return resp.get("Items", [])

    def delete_contact(self, contact_id: str) -> None:
        """Hard-delete a contact."""
        self.table.delete_item(
            Key={
                "PK": self._pk("CONTACT"),
                "SK": f"CONTACT#{contact_id}",
            }
        )

    # ── Briefings ─────────────────────────────────────────────────────────────
    # Saved deep-dive briefings for a contact. Stored under a per-tenant
    # partition with timestamped sort keys for chronological listing.
    #
    # PK: TENANT#{tid}#BRIEFING
    # SK: BRIEFING#CONTACT#{contact_id}#{iso_timestamp}#{briefing_id}

    def put_briefing(
        self,
        contact_id: str,
        contact_name: str,
        focus: str,
        markdown: str,
        model: str = "",
        tokens_in: int = 0,
        tokens_out: int = 0,
    ) -> dict:
        """Save a deep-dive briefing for a contact."""
        briefing_id = str(uuid.uuid4())
        now = self._now_iso()
        item = {
            "PK": self._pk("BRIEFING"),
            "SK": f"BRIEFING#CONTACT#{contact_id}#{now}#{briefing_id}",
            "briefingId": briefing_id,
            "contactId": contact_id,
            "contactName": contact_name,
            "tenantId": self.tenant_id,
            "focus": focus or "",
            "markdown": markdown,
            "model": model,
            "tokensIn": tokens_in,
            "tokensOut": tokens_out,
            "createdAt": now,
        }
        self.table.put_item(Item=item)
        return item

    def list_briefings_for_contact(self, contact_id: str, limit: int = 50) -> list[dict]:
        """List all saved briefings for a single contact, newest first."""
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("BRIEFING"))
                & Key("SK").begins_with(f"BRIEFING#CONTACT#{contact_id}#")
            ),
            ScanIndexForward=False,  # newest first (timestamps in SK)
            Limit=limit,
        )
        return resp.get("Items", [])

    def get_briefing(self, contact_id: str, briefing_id: str) -> dict | None:
        """Fetch a specific briefing by id (requires scanning the contact prefix)."""
        items = self.list_briefings_for_contact(contact_id, limit=200)
        for item in items:
            if item.get("briefingId") == briefing_id:
                return item
        return None

    def delete_briefing(self, contact_id: str, briefing_id: str) -> bool:
        """Delete a briefing by id. Returns True if deleted."""
        item = self.get_briefing(contact_id, briefing_id)
        if not item:
            return False
        self.table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
        return True

    # ── Conversations ─────────────────────────────────────────────────────────

    def put_conversation(self, user_id: str, conv_id: str, title: str = "") -> dict:
        """Create or update a conversation record.

        Args:
            user_id: Cognito sub of the user who owns this conversation.
            conv_id: Unique conversation identifier.
            title:   Short human-readable title (first user message or explicit label).

        Returns:
            The conversation item as stored.
        """
        now = self._now_iso()
        item = {
            "PK": self._pk("CONV"),
            "SK": f"CONV#{conv_id}",
            # GSI2 — time-sorted conversations for this tenant
            "GSI2PK": self._pk("CONV"),
            "GSI2SK": now,
            "convId": conv_id,
            "tenantId": self.tenant_id,
            "userId": user_id,
            "title": title or "Untitled conversation",
            "createdAt": now,
            "updatedAt": now,
            "messageCount": 0,
        }
        self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
        return item

    def get_conversation(self, conv_id: str) -> dict | None:
        """Fetch conversation metadata. Returns None if not found."""
        resp = self.table.get_item(
            Key={
                "PK": self._pk("CONV"),
                "SK": f"CONV#{conv_id}",
            }
        )
        return resp.get("Item")

    def list_conversations(self, user_id: str, limit: int = 20) -> list[dict]:
        """List conversations for a user, most recent first.

        Uses GSI2 (time-based) so results are naturally date-sorted.
        """
        resp = self.table.query(
            IndexName="GSI2",
            KeyConditionExpression=Key("GSI2PK").eq(self._pk("CONV")),
            FilterExpression="userId = :uid",
            ExpressionAttributeValues={":uid": user_id},
            Limit=limit,
            ScanIndexForward=False,  # newest first
        )
        return resp.get("Items", [])

    def list_all_conversations(self, limit: int = 50) -> list[dict]:
        """List conversations for the entire tenant (all users), newest first.

        Used by the super-admin drilldown so Edward can see every prompt
        a tenant's users have sent — not just the ones belonging to the
        caller's own user_id. No filter expression, so every row in the
        GSI2 partition comes back up to the limit.
        """
        resp = self.table.query(
            IndexName="GSI2",
            KeyConditionExpression=Key("GSI2PK").eq(self._pk("CONV")),
            Limit=limit,
            ScanIndexForward=False,
        )
        return resp.get("Items", [])

    # ── Messages ──────────────────────────────────────────────────────────────

    def append_message(
        self,
        conv_id: str,
        role: str,
        content: str,
        tokens: int = 0,
    ) -> dict:
        """Append a single message to a conversation.

        Args:
            conv_id:  Conversation identifier.
            role:     "user" | "assistant" | "tool".
            content:  Message text.
            tokens:   Token count for cost tracking (0 if unavailable).

        Returns:
            The message item as stored.
        """
        now = self._now_iso()
        msg_id = str(uuid.uuid4())
        item = {
            "PK": f"{self._pk('CONV')}#{conv_id}#MSG",
            "SK": f"MSG#{now}#{msg_id}",
            "convId": conv_id,
            "tenantId": self.tenant_id,
            "msgId": msg_id,
            "role": role,
            "content": content,
            "tokens": tokens,
            "createdAt": now,
        }
        self.table.put_item(Item=item)

        # Update conversation message count + updatedAt
        try:
            self.table.update_item(
                Key={
                    "PK": self._pk("CONV"),
                    "SK": f"CONV#{conv_id}",
                },
                UpdateExpression="SET updatedAt = :now ADD messageCount :one",
                ExpressionAttributeValues={":now": now, ":one": 1},
            )
        except Exception:
            pass  # non-fatal — message was written

        return item

    def get_messages(self, conv_id: str, limit: int = 100) -> list[dict]:
        """Fetch all messages for a conversation, oldest first."""
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(f"{self._pk('CONV')}#{conv_id}#MSG")
                & Key("SK").begins_with("MSG#")
            ),
            Limit=limit,
            ScanIndexForward=True,  # chronological
        )
        return resp.get("Items", [])

    # ── Usage tracking ────────────────────────────────────────────────────────

    def record_usage(
        self,
        tokens_in: int,
        tokens_out: int,
        cost: float,
    ) -> None:
        """Increment today's token and cost totals for this tenant.

        Uses atomic ADD so concurrent Lambda invocations don't stomp each other.
        """
        today = date.today().isoformat()
        self.table.update_item(
            Key={
                "PK": self._pk("USAGE"),
                "SK": f"DATE#{today}",
            },
            UpdateExpression=(
                "SET tenantId = :tid, #date = :date "
                "ADD tokensIn :ti, tokensOut :to, costUsd :cost, #calls :one"
            ),
            ExpressionAttributeNames={
                "#date": "date",
                "#calls": "calls",
            },
            ExpressionAttributeValues={
                ":tid": self.tenant_id,
                ":date": today,
                ":ti": tokens_in,
                ":to": tokens_out,
                ":cost": Decimal(str(round(cost, 8))),
                ":one": 1,
            },
        )

    def get_usage(self, query_date: str | None = None) -> dict | None:
        """Fetch usage record for a date (ISO format). Defaults to today."""
        target = query_date or date.today().isoformat()
        resp = self.table.get_item(
            Key={
                "PK": self._pk("USAGE"),
                "SK": f"DATE#{target}",
            }
        )
        return resp.get("Item")

    def get_usage_month(self, year_month: str) -> list[dict]:
        """Fetch all usage records for a month, e.g. '2026-03'.

        Returns list of daily records sorted oldest-first.
        """
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("USAGE"))
                & Key("SK").begins_with(f"DATE#{year_month}")
            ),
            ScanIndexForward=True,
        )
        return resp.get("Items", [])

    # ── Per-user usage tracking ───────────────────────────────────────────────
    # Mirrors record_usage / get_usage_month but keyed by user_id so we can
    # attribute token spend to individual users in a tenant (e.g. show each
    # user their own monthly token counts on the Settings page).
    #
    # Schema:
    #   PK = TENANT#{tid}#USER_USAGE
    #   SK = USER#{user_id}#DATE#{yyyy-mm-dd}

    def record_user_usage(
        self,
        user_id: str,
        tokens_in: int,
        tokens_out: int,
        cost: float,
    ) -> None:
        """Increment today's token + cost totals for a specific user.

        Atomic ADD so concurrent invocations don't stomp each other. No-op if
        user_id is empty (some early requests have no identity resolved).
        """
        if not user_id:
            return
        today = date.today().isoformat()
        self.table.update_item(
            Key={
                "PK": self._pk("USER_USAGE"),
                "SK": f"USER#{user_id}#DATE#{today}",
            },
            UpdateExpression=(
                "SET tenantId = :tid, userId = :uid, #date = :date "
                "ADD tokensIn :ti, tokensOut :to, costUsd :cost, #calls :one"
            ),
            ExpressionAttributeNames={
                "#date": "date",
                "#calls": "calls",
            },
            ExpressionAttributeValues={
                ":tid": self.tenant_id,
                ":uid": user_id,
                ":date": today,
                ":ti": tokens_in,
                ":to": tokens_out,
                ":cost": Decimal(str(round(cost, 8))),
                ":one": 1,
            },
        )

    def get_user_usage_month(self, user_id: str, year_month: str) -> list[dict]:
        """Fetch one user's daily usage records for a month. Sorted oldest-first."""
        if not user_id:
            return []
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("USER_USAGE"))
                & Key("SK").begins_with(f"USER#{user_id}#DATE#{year_month}")
            ),
            ScanIndexForward=True,
        )
        return resp.get("Items", [])

    def list_all_user_usage_month(self, year_month: str) -> list[dict]:
        """Fetch every user's usage records in this tenant for a given month.

        Used by the tenant admin view to compare token spend across users.
        Performs a single query on the USER_USAGE partition and filters by
        the date suffix client-side since the SK is prefixed by user, not
        date.
        """
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("USER_USAGE"))
                & Key("SK").begins_with("USER#")
            ),
        )
        items = resp.get("Items", [])
        # Filter on the ISO date that was stored as an attribute
        needle = year_month
        return [i for i in items if str(i.get("date", "")).startswith(needle)]

    # ── Per-user tool usage tracking ──────────────────────────────────────────
    # One record per (user, tool, month) with an atomic ADD counter. Matches
    # the pattern of record_user_usage so the drilldown can show "al-3d0efddd
    # called research_person_deep 11 times in 2026-04".
    #
    # Schema:
    #   PK = TENANT#{tid}#USER_TOOL_USAGE
    #   SK = USER#{user_id}#TOOL#{tool_name}#YM#{yyyy-mm}

    def record_user_tool_usage(
        self,
        user_id: str,
        tool_counts: dict[str, int],
        year_month: str | None = None,
    ) -> None:
        """Increment tool-call counts for a user within a given month.

        Args:
            user_id:     Cognito sub of the caller.
            tool_counts: {tool_name: times_called_in_this_turn}.
            year_month:  YYYY-MM, defaults to current month.

        No-op if user_id is empty or tool_counts is empty. Each tool gets
        its own update_item so a crash on one doesn't lose the others.
        """
        if not user_id or not tool_counts:
            return
        ym = year_month or date.today().strftime("%Y-%m")
        for tool_name, count in tool_counts.items():
            if not tool_name or count <= 0:
                continue
            # Defensive: DynamoDB attribute values can't contain '#' in the
            # key component but tool_name goes into the SK, so any exotic
            # chars are safe here. We still trim to 80 chars to bound size.
            safe_name = tool_name[:80]
            try:
                self.table.update_item(
                    Key={
                        "PK": self._pk("USER_TOOL_USAGE"),
                        "SK": f"USER#{user_id}#TOOL#{safe_name}#YM#{ym}",
                    },
                    UpdateExpression=(
                        "SET tenantId = :tid, userId = :uid, "
                        "toolName = :tn, yearMonth = :ym "
                        "ADD #calls :n"
                    ),
                    ExpressionAttributeNames={"#calls": "calls"},
                    ExpressionAttributeValues={
                        ":tid": self.tenant_id,
                        ":uid": user_id,
                        ":tn": safe_name,
                        ":ym": ym,
                        ":n": int(count),
                    },
                )
            except Exception:
                # Best effort — never break writeback on one tool failure.
                continue

    def list_user_tool_usage_month(
        self, user_id: str, year_month: str
    ) -> list[dict]:
        """Fetch tool-call counts for one user in a given month."""
        if not user_id:
            return []
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("USER_TOOL_USAGE"))
                & Key("SK").begins_with(f"USER#{user_id}#TOOL#")
            ),
        )
        items = resp.get("Items", [])
        return [i for i in items if str(i.get("yearMonth", "")) == year_month]

    def list_all_user_tool_usage_month(self, year_month: str) -> list[dict]:
        """Fetch every user's tool counts in this tenant for a given month.

        One row per (user, tool). The drilldown groups client-side.
        """
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("USER_TOOL_USAGE"))
                & Key("SK").begins_with("USER#")
            ),
        )
        items = resp.get("Items", [])
        return [i for i in items if str(i.get("yearMonth", "")) == year_month]

    # ── Per-user model usage tracking ─────────────────────────────────────────
    # One row per (user, model, month). Same shape as tool usage so the
    # drilldown can pivot client-side. Lets Edward see "al-3d0efddd used
    # Sonnet for 95% of tokens, Opus for 5%" — important because Opus costs
    # ~5x Sonnet and cost surprises hide in the mix.
    #
    # Schema:
    #   PK = TENANT#{tid}#USER_MODEL_USAGE
    #   SK = USER#{user_id}#MODEL#{model_id}#YM#{yyyy-mm}

    def record_user_model_usage(
        self,
        user_id: str,
        model_id: str,
        tokens_in: int,
        tokens_out: int,
        year_month: str | None = None,
    ) -> None:
        """Increment monthly token counts for a (user, model) pair."""
        if not user_id or not model_id:
            return
        ym = year_month or date.today().strftime("%Y-%m")
        safe_model = model_id[:120]
        try:
            self.table.update_item(
                Key={
                    "PK": self._pk("USER_MODEL_USAGE"),
                    "SK": f"USER#{user_id}#MODEL#{safe_model}#YM#{ym}",
                },
                UpdateExpression=(
                    "SET tenantId = :tid, userId = :uid, "
                    "modelId = :mid, yearMonth = :ym "
                    "ADD tokensIn :ti, tokensOut :to, #calls :one"
                ),
                ExpressionAttributeNames={"#calls": "calls"},
                ExpressionAttributeValues={
                    ":tid": self.tenant_id,
                    ":uid": user_id,
                    ":mid": safe_model,
                    ":ym": ym,
                    ":ti": int(tokens_in),
                    ":to": int(tokens_out),
                    ":one": 1,
                },
            )
        except Exception:
            pass

    def list_all_user_model_usage_month(self, year_month: str) -> list[dict]:
        """Fetch every user's model breakdown in this tenant for a given month."""
        resp = self.table.query(
            KeyConditionExpression=(
                Key("PK").eq(self._pk("USER_MODEL_USAGE"))
                & Key("SK").begins_with("USER#")
            ),
        )
        items = resp.get("Items", [])
        return [i for i in items if str(i.get("yearMonth", "")) == year_month]
