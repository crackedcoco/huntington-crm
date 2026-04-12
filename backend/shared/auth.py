"""Auth helpers — extract tenant context from API Gateway JWT claims.

API Gateway (HTTP API with JWT authorizer) populates:
  event["requestContext"]["authorizer"]["jwt"]["claims"]

API Gateway (REST API with Cognito authorizer) populates:
  event["requestContext"]["authorizer"]["claims"]

Both formats are handled transparently.
"""
from __future__ import annotations


def extract_tenant(event: dict) -> dict:
    """Extract tenant context from an API Gateway Lambda event.

    Works with both REST API (Cognito authorizer) and HTTP API (JWT authorizer)
    event formats. Custom Cognito attributes are prefixed 'custom:' in the JWT.

    Args:
        event: Raw Lambda event dict from API Gateway.

    Returns:
        Dict with keys:
          tenant_id  — str | None
          plan       — str  (default "starter")
          role       — str  (default "member")
          user_id    — str | None  (Cognito sub)
          email      — str | None
    """
    authorizer = event.get("requestContext", {}).get("authorizer", {})

    # HTTP API JWT authorizer nests claims under "jwt"
    claims: dict = authorizer.get("jwt", {}).get("claims") or authorizer.get("claims") or {}

    return {
        "tenant_id": claims.get("custom:tenant_id"),
        "plan": claims.get("custom:plan", "starter"),
        "role": claims.get("custom:role", "member"),
        "user_id": claims.get("sub"),
        "email": claims.get("email"),
    }


def require_tenant(event: dict) -> dict:
    """Like extract_tenant but raises ValueError if tenant_id is missing.

    Use this in Lambda handlers where a valid tenant is mandatory.

    Raises:
        ValueError: If the JWT is missing a tenant_id claim.
    """
    ctx = extract_tenant(event)
    if not ctx["tenant_id"]:
        raise ValueError("JWT is missing custom:tenant_id — user account may not be fully provisioned")
    return ctx
