# Huntington CRM

Standalone CRM spun off from the Jennifer project. Contact management, pipeline tracking, business intelligence, and prospect scoring for Huntington Analytics.

## Architecture

### Data Layer (dual-write)
- **SQLite** (`~/.jennifer/contacts.db`) — authoritative write path, offline-safe, fast
- **Postgres** (`business_intel`) — unified read layer with 165+ contacts, 71K businesses, 800K reviews
- **DynamoDB** (`jennjim-main-{stage}`) — multi-tenant API backend (shared with Jennifer for now)

### Code Layout
```
backend/crm/handler.py      — DynamoDB REST API (Lambda handler)
backend/shared/              — auth, db, features, rate_limit, response (forked from Jennifer)
local/contacts_db.py         — SQLite CRM + Postgres unified queries
local/pg_mirror.py           — write-through replication to business_intel
dashboard/index.html         — web UI (deploy target: crm.huntington-analytics.com)
infrastructure/stacks/       — CDK stack (TODO)
```

### API Endpoints
```
GET    /api/contacts              — list contacts (paginated, searchable)
POST   /api/contacts              — create contact
GET    /api/contacts/{id}         — get contact + audits + interactions
PUT    /api/contacts/{id}         — update contact
DELETE /api/contacts/{id}         — soft delete
POST   /api/contacts/{id}/audits  — create audit (auto-logs interaction)
GET    /api/audits                — list audits
POST   /api/contacts/{id}/interactions — log interaction
GET    /api/interactions          — list interactions
```

### Deploy Target
- **Web UI:** `crm.huntington-analytics.com` (S3 + CloudFront)
- **SSL:** `*.huntington-analytics.com` wildcard cert (arn:aws:acm:us-east-1:168362164058:certificate/b9920d78-85e4-4ac3-925b-d91903ec333a)
- **AWS user:** amplify-dev

### Deploying Dashboard
```bash
aws s3 sync dashboard/ s3://crm.huntington-analytics.com/ --region us-east-1
aws cloudfront create-invalidation --distribution-id <TBD> --paths "/*"
```

## Key Tables (Postgres business_intel)
- `contacts` — 165+ rows across all sources (Jennifer, crm-db RDS imports)
- `businesses` — 71K scraped businesses
- `reviews` — 800K+ reviews
- `business_health` — scored prospects (5,274 rows)
- `audits`, `interactions`, `research` — mirrored from SQLite

## Key Tables (SQLite ~/.jennifer/contacts.db)
- `contacts` — 40 personal contacts (business cards, meetings, referrals)
- `audits`, `research`, `interactions` — linked to contacts
- `business_health`, `health_benchmarks` — local scoring cache

## Conventions
1. SQLite is the write path. Postgres is the read layer.
2. Every write mirrors to Postgres via `pg_mirror.py`. If Postgres is down, writes queue to `~/.jennifer/pg_replay.jsonl`.
3. Never delete contacts — soft delete only.
4. The dashboard must work offline against SQLite if the API is unavailable.
