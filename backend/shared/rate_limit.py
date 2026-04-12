"""Per-tenant rate limiting using DynamoDB atomic counters."""
from datetime import UTC, datetime
import boto3
from boto3.dynamodb.conditions import Key


def check_rate_limit(table_name: str, tenant_id: str, window_seconds: int = 60, max_requests: int = 30) -> bool:
    """Check if tenant is within rate limit. Returns True if allowed, False if throttled."""
    table = boto3.resource("dynamodb").Table(table_name)
    now = datetime.now(UTC)
    window_key = now.strftime("%Y-%m-%dT%H:%M")  # 1-minute windows

    pk = f"TENANT#{tenant_id}#RATELIMIT"
    sk = f"WINDOW#{window_key}"

    try:
        resp = table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression="ADD #cnt :one SET #ttl = :ttl",
            ExpressionAttributeNames={"#cnt": "count", "#ttl": "ttl"},
            ExpressionAttributeValues={
                ":one": 1,
                ":ttl": int(now.timestamp()) + window_seconds + 60,  # TTL for DynamoDB cleanup
            },
            ReturnValues="UPDATED_NEW",
        )
        count = int(resp["Attributes"]["count"])
        return count <= max_requests
    except Exception:
        return True  # Fail open — don't block users on rate limit infrastructure failure
