"""API response helpers for Lambda handlers.

All responses include the standard CORS headers for app.huntington-analytics.com.
Import the convenience functions (success, error, forbidden) rather than
constructing raw dicts in every handler.

Usage:
    from backend.shared.response import success, error, forbidden

    return success({"conversations": items})
    return error("Missing required field: message")
    return forbidden("Card pipeline requires Growth plan or higher.")
"""
from __future__ import annotations

import json
import os

# Configured via Lambda environment; falls back to the production origin.
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "https://app.huntington-analytics.com")


def api_response(
    status_code: int,
    body: dict | list,
    headers: dict[str, str] | None = None,
) -> dict:
    """Build a fully-formed API Gateway Lambda response dict.

    Args:
        status_code: HTTP status code (200, 400, 403, 500, …).
        body:        JSON-serialisable response body.
        headers:     Optional extra headers to merge in.

    Returns:
        Dict with statusCode, headers, and body (JSON string).
    """
    response_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": CORS_ORIGIN,
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    }
    if headers:
        response_headers.update(headers)

    return {
        "statusCode": status_code,
        "headers": response_headers,
        "body": json.dumps(body, default=str),  # default=str handles Decimal / datetime
    }


def success(body: dict | list) -> dict:
    """200 OK response.

    Args:
        body: JSON-serialisable response payload.
    """
    return api_response(200, body)


def created(body: dict | list) -> dict:
    """201 Created response."""
    return api_response(201, body)


def error(message: str, code: int = 400) -> dict:
    """Client error response (4xx).

    Args:
        message: Human-readable error description.
        code:    HTTP status code. Defaults to 400.
    """
    return api_response(code, {"error": message})


def forbidden(message: str = "Upgrade required") -> dict:
    """403 Forbidden response — used for plan-gated features.

    Includes an 'upgrade' flag so the frontend can show an upgrade prompt.

    Args:
        message: Why the request was denied.
    """
    return api_response(403, {"error": message, "upgrade": True})


def not_found(resource: str = "Resource") -> dict:
    """404 Not Found response.

    Args:
        resource: Name of the missing resource for the error message.
    """
    return api_response(404, {"error": f"{resource} not found"})


def server_error(message: str = "Internal server error") -> dict:
    """500 Internal Server Error response."""
    return api_response(500, {"error": message})


def options_preflight() -> dict:
    """200 response for CORS OPTIONS preflight requests."""
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ORIGIN,
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            "Access-Control-Max-Age": "3600",
        },
        "body": "",
    }
