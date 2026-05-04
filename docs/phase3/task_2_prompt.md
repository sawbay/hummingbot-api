# Task 3.2 — Database Query Optimization & Caching

> **Agent prompt** for adding DB indexes to the `executors` table, implementing a TTL
> cache for `get_executors()`, and adding pagination to the executor search endpoint.

---

You are a backend Python engineer working on the `hummingbot-api` repository.

## Context

The `ExecutorRecord` model is in `database/models.py` (the `executors` table).
The repository is in `database/repositories/executor_repository.py`.
The service that calls the repository is `services/executor_service.py`.
The REST endpoint for executor search is in `routers/executors.py`.

The project does **not** use Alembic. Tables are created via:
```python
await db_manager.create_tables()  # calls Base.metadata.create_all()
```
in `main.py` lifespan. Since SQLAlchemy's `create_all` only adds new tables and does not
modify existing ones, **new indexes on existing tables must be applied via a raw SQL
migration script** rather than by changing the model alone.

The `ExecutorRecord` model currently has these columns (all with `index=True` already
where noted):
- `executor_id` — `unique=True, index=True`
- `executor_type` — `index=True`
- `account_name` — `index=True`
- `connector_name` — `index=True`
- `trading_pair` — `index=True`
- `controller_id` — `index=True`
- `created_at` — `index=True`
- `closed_at` — `index=True`
- `status` — `index=True`

The `get_executors()` method in `ExecutorService` combines in-memory active executors
with a DB query via `repo.get_executors(...)`. It is called by every poll iteration of
`_executors_push_loop` in `ExecutorWebSocketManager`.

There is no existing caching layer. The `ExecutorService` is a singleton created in
`main.py` lifespan.

## Task

Add a 1-second TTL in-memory cache to `ExecutorService.get_executors()`, add a
composite DB index for the most common combined filter query, add pagination to the
`POST /executors/search` endpoint, and create a SQL migration script for the new index.

### Changes required:

#### 1. Add TTL cache to `ExecutorService` in `services/executor_service.py`

Add these private attributes to `ExecutorService.__init__()`:
```python
# Executor list cache
self._executor_cache: list = []
self._executor_cache_filters: dict = {}
self._executor_cache_ts: float = 0.0
self._executor_cache_ttl: float = 1.0  # seconds
```

Refactor `get_executors()` to use the cache:

```python
async def get_executors(
    self,
    account_name=None,
    connector_name=None,
    trading_pair=None,
    executor_type=None,
    status=None,
    controller_id=None,
    limit=None,
) -> list:
    import time as _time

    # Build a cache key from the filter args
    cache_key = {
        "account_name": account_name,
        "connector_name": connector_name,
        "trading_pair": trading_pair,
        "executor_type": executor_type,
        "status": status,
        "controller_id": controller_id,
        "limit": limit,
    }

    now = _time.monotonic()
    if (
        now - self._executor_cache_ts < self._executor_cache_ttl
        and self._executor_cache_filters == cache_key
    ):
        return self._executor_cache

    # Cache miss — fetch fresh data
    result = await self._fetch_executors(
        account_name=account_name,
        connector_name=connector_name,
        trading_pair=trading_pair,
        executor_type=executor_type,
        status=status,
        controller_id=controller_id,
        limit=limit,
    )

    self._executor_cache = result
    self._executor_cache_filters = cache_key
    self._executor_cache_ts = now

    return result
```

Rename the existing body of `get_executors()` to `_fetch_executors()` with the same
signature. Keep the logic identical — only the name changes.

Also add an `invalidate_executor_cache()` method:
```python
def invalidate_executor_cache(self) -> None:
    """Invalidate the executor list cache (call after create/stop operations)."""
    self._executor_cache_ts = 0.0
```

Call `self.invalidate_executor_cache()` at the end of `create_executor()` and at the
end of `_handle_executor_completion()` to ensure the cache reflects new state
immediately after changes.

#### 2. Add pagination to `POST /executors/search` in `routers/executors.py`

Read `routers/executors.py` in full first to understand the current request/response
models and endpoint structure.

Find the `POST /executors/search` endpoint (or equivalent search endpoint). Add
`limit: int = 100` and `offset: int = 0` to the request body model (in `models/executors.py`
or inline — wherever the search request model is defined).

In the endpoint handler:
1. Pass `limit` to `executor_service.get_executors(limit=limit)`.
2. Apply `offset` in Python after fetching: `paginated = result[offset:offset + limit]`.
3. Return `total_count` alongside the paginated results:
   ```python
   return {
       "status": "success",
       "data": paginated,
       "total_count": len(result),
       "limit": limit,
       "offset": offset,
   }
   ```

Also add `limit` and `offset` to the `executors` WS subscription in
`services/executor_ws_manager.py`:
- In `handle_subscribe` for `sub_type == "executors"`: read
  `sub.filters.get("limit", 100)` and `sub.filters.get("offset", 0)`.
- Store them in `sub.filters` (they are already stored as-is via `sub.filters = filters`).
- In `_executors_push_loop` (or `_fetch_executors` if refactored), pass
  `limit=sub.filters.get("limit", 100)` to `get_executors()`.
- Apply `offset` slice after fetching.

#### 3. Create a composite DB index migration script

Create `database/migrations/001_add_executor_composite_index.sql`:

```sql
-- Migration 001: Add composite index on executors table for common filter patterns
-- Run once against the target PostgreSQL database.
-- Safe to run multiple times (uses IF NOT EXISTS).

CREATE INDEX IF NOT EXISTS idx_executors_controller_status
    ON executors (controller_id, status);

CREATE INDEX IF NOT EXISTS idx_executors_account_connector
    ON executors (account_name, connector_name);

CREATE INDEX IF NOT EXISTS idx_executors_created_at_desc
    ON executors (created_at DESC);

-- Verify indexes were created
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'executors'
ORDER BY indexname;
```

Also create `database/migrations/README.md`:
```markdown
# Database Migrations

This project does not use Alembic. Migrations are plain SQL scripts run manually
or via CI against the target PostgreSQL database.

## Running a migration

```bash
psql $DATABASE_URL -f database/migrations/001_add_executor_composite_index.sql
```

## Migration files

| File | Description |
|---|---|
| `001_add_executor_composite_index.sql` | Composite indexes on `executors` table for common filter queries |
```

#### 4. Benchmark note

Add a comment at the top of the migration SQL file with the expected impact:
```sql
-- Expected impact:
-- Before: full sequential scan O(N) on 10k records ≈ 200-500ms
-- After:  index scan O(log N) ≈ 5-20ms for filtered queries
-- Benchmark with: EXPLAIN ANALYZE SELECT * FROM executors WHERE controller_id='x' AND status='CLOSED';
```

### Acceptance criteria:
- Two consecutive calls to `get_executors()` with identical filters within 1 second
  return the same result without hitting the database (verify by adding a debug log
  `"Cache hit for get_executors"` inside the cache branch).
- After `create_executor()` or executor completion, the next `get_executors()` call
  fetches fresh data from the database (cache invalidated).
- `POST /executors/search` with `{"limit": 10, "offset": 20}` returns at most 10 records
  starting from position 20, plus a `total_count` field.
- `total_count` reflects the total number of executors matching the filter (before
  pagination), not just the current page size.
- The SQL migration file runs cleanly against a PostgreSQL database with `psql`.
- The `executors` WS subscription respects `limit` and `offset` from the subscribe
  message filters.
- No behavioral change for callers that don't pass `limit`/`offset` (default values apply).
- Use `logger = logging.getLogger(__name__)` for all new log lines.
- Wrap all DB access in try/except following the existing patterns in `executor_service.py`.

Read `services/executor_service.py`, `routers/executors.py`, `database/models.py`,
`database/repositories/executor_repository.py`, and `services/executor_ws_manager.py`
in full before making any changes.
