"""Feature flags and plan limits for Jenn & Jim SaaS.

Plans:
  starter  — entry-level, conversation + basic scanning
  growth   — full CRM, card pipeline, dossiers, integrations
  scale    — everything, high limits (sales / agency tier)

Usage:
    from backend.shared.features import check_feature, check_limit

    if not check_feature(plan, "card_pipeline"):
        return forbidden("Card pipeline is a Growth feature. Time to upgrade.")

    if not check_limit(plan, "contacts", current_count):
        return forbidden("Contact limit reached for your plan.")
"""
from __future__ import annotations

# ── Feature gates ─────────────────────────────────────────────────────────────

FEATURES: dict[str, set[str]] = {
    "starter": {
        "chat",
        "scan_website",
        "contacts_basic",
    },
    "growth": {
        "chat",
        "scan_website",
        "contacts_full",
        "card_pipeline",
        "dossier",
        "health_index",
        "email_integration",
        "meeting_transcription",
    },
    "scale": {
        # Wildcard — all features enabled
        "*",
    },
}

# ── Plan limits ───────────────────────────────────────────────────────────────
# -1 means unlimited.

LIMITS: dict[str, dict[str, int]] = {
    "starter": {
        "conversations_per_month": 50,
        "scans_per_month": 10,
        "contacts": 100,
    },
    "growth": {
        "conversations_per_month": 200,
        "scans_per_month": 50,
        "contacts": 500,
    },
    "scale": {
        "conversations_per_month": 1000,
        "scans_per_month": -1,
        "contacts": -1,
    },
}


# ── Public helpers ────────────────────────────────────────────────────────────


def check_feature(plan: str, feature: str) -> bool:
    """Return True if the plan has access to the given feature.

    Args:
        plan:    Tenant plan slug ("starter", "growth", "scale").
        feature: Feature identifier string.

    Returns:
        True if the feature is available on this plan.

    Examples:
        >>> check_feature("starter", "chat")
        True
        >>> check_feature("starter", "card_pipeline")
        False
        >>> check_feature("scale", "anything")
        True
    """
    allowed = FEATURES.get(plan, set())
    return "*" in allowed or feature in allowed


def check_limit(plan: str, limit_name: str, current_value: int) -> bool:
    """Return True if current_value is within the plan's limit.

    Args:
        plan:          Tenant plan slug.
        limit_name:    Limit key (e.g. "contacts", "scans_per_month").
        current_value: Current usage count to check against the limit.

    Returns:
        True if usage is below the limit (or the limit is -1 / unlimited).

    Examples:
        >>> check_limit("starter", "contacts", 99)
        True
        >>> check_limit("starter", "contacts", 100)
        False
        >>> check_limit("scale", "scans_per_month", 99999)
        True
    """
    limits = LIMITS.get(plan, {})
    max_val = limits.get(limit_name, 0)
    return max_val == -1 or current_value < max_val


def get_limit(plan: str, limit_name: str) -> int:
    """Return the raw limit value for a plan (-1 = unlimited, 0 = unknown)."""
    return LIMITS.get(plan, {}).get(limit_name, 0)


def get_features(plan: str) -> set[str]:
    """Return the set of feature strings for a plan."""
    return FEATURES.get(plan, set())
