# Task 3.1 — JWT Bearer Token Authentication

> **Agent prompt** for replacing the global HTTP Basic Auth with JWT Bearer tokens,
> adding a `POST /auth/token` endpoint, and securing WebSocket connections at handshake time.

---

You are a backend Python engineer working on the `hummingbot-api` repository.

## Context

`main.py` uses a single global `username`/`password` read from `.env` via `config.py`
(`settings.security.username` and `settings.security.password`). Every protected endpoint
uses `Depends(auth_user)` where `auth_user` is an `HTTPBasic` dependency defined in
`main.py` around line 356.

`routers/websocket.py` has its own `_authenticate_websocket()` function that also checks
Basic Auth from the `Authorization` header or `?token=base64(user:pass)` query param.

The `SecuritySettings` class in `config.py` currently has:
- `username: str`
- `password: str`
- `debug_mode: bool`
- `config_password: str`

There is no Alembic migration system — the project uses `await db_manager.create_tables()`
in `main.py` lifespan which calls `Base.metadata.create_all`.

## Task

Replace per-request Basic Auth with JWT Bearer tokens. Add a token issuance endpoint.
Keep Basic Auth working **only** on `POST /auth/token` for backward compatibility.

### Changes required:

#### 1. Add `jwt_secret` to `SecuritySettings` in `config.py`

```python
jwt_secret: str = Field(
    default="change-me-in-production-use-a-long-random-string",
    description="Secret key for signing JWT tokens"
)
jwt_expire_minutes: int = Field(
    default=60,
    description="JWT token expiry in minutes"
)
```

The `model_config` in `SecuritySettings` already uses `env_prefix=""` so these will read
from `.env` as `JWT_SECRET` and `JWT_EXPIRE_MINUTES`.

#### 2. Create `utils/auth.py` (new file)

```python
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from fastapi import HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import settings

logger = logging.getLogger(__name__)

ALGORITHM = "HS256"

bearer_scheme = HTTPBearer(auto_error=False)


def create_access_token(username: str) -> str:
    """Create a signed JWT access token."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + timedelta(minutes=settings.security.jwt_expire_minutes),
    }
    return jwt.encode(payload, settings.security.jwt_secret, algorithm=ALGORITHM)


def verify_token(token: str) -> dict:
    """Decode and verify a JWT token. Raises HTTPException on failure."""
    try:
        payload = jwt.decode(
            token,
            settings.security.jwt_secret,
            algorithms=[ALGORITHM],
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )


def verify_jwt_token(
    credentials: HTTPAuthorizationCredentials = None,
) -> str:
    """
    FastAPI dependency: extract and verify JWT from Authorization: Bearer <token> header.
    Returns the username (sub claim) on success.
    Raises HTTP 401 if token is missing, expired, or invalid.
    """
    if settings.security.debug_mode:
        return "debug_user"

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_token(credentials.credentials)
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing 'sub' claim",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return username
```

Install `PyJWT` if not already in `environment.yml`:
- Check `environment.yml` for `PyJWT` or `pyjwt`. If missing, add `- pyjwt>=2.8.0`
  under the `pip:` section.

#### 3. Create `routers/auth.py` (new file)

```python
import secrets
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from config import settings
from utils.auth import create_access_token

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Authentication"], prefix="/auth")

_basic_security = HTTPBasic()


@router.post("/token")
def issue_token(credentials: HTTPBasicCredentials = Depends(_basic_security)):
    """
    Issue a JWT Bearer token in exchange for valid Basic Auth credentials.

    This is the only endpoint that accepts Basic Auth. All other endpoints
    require a Bearer token obtained from this endpoint.
    """
    correct_username = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        settings.security.username.encode("utf-8"),
    )
    correct_password = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        settings.security.password.encode("utf-8"),
    )

    if not (correct_username and correct_password) and not settings.security.debug_mode:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    token = create_access_token(credentials.username)
    logger.info(f"Issued JWT token for user: {credentials.username}")

    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": settings.security.jwt_expire_minutes * 60,
    }
```

#### 4. Update `main.py`

**a) Replace the `auth_user` dependency** (around line 356):

Remove the existing `auth_user` function and its `security = HTTPBasic()` line.

Replace with:
```python
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from utils.auth import bearer_scheme, verify_jwt_token

def auth_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> str:
    """Authenticate user via JWT Bearer token."""
    return verify_jwt_token(credentials)
```

**b) Register the auth router** (near the `app.include_router(health.router)` line):

Add these imports at the top with the other router imports:
```python
from routers import auth
```

Add immediately after `app.include_router(health.router)`:
```python
# Auth token endpoint (Basic Auth only — used to obtain JWT)
app.include_router(auth.router)
```

Do NOT add `Depends(auth_user)` to the auth router — it uses its own Basic Auth.

**c) No changes needed** to the `app.include_router(...)` calls for other routers —
they already use `dependencies=[Depends(auth_user)]` and `auth_user` will now verify JWT.

#### 5. Update `routers/websocket.py`

Replace the `_authenticate_websocket()` function with a JWT-aware version:

```python
from utils.auth import verify_token

def _authenticate_websocket(websocket: WebSocket) -> bool:
    """
    Authenticate a WebSocket connection using a JWT Bearer token.

    Accepts the token via:
    - Authorization: Bearer <token> header
    - ?token=<jwt> query parameter
    """
    if settings.security.debug_mode:
        return True

    # Try Authorization: Bearer <token> header
    auth_header = websocket.headers.get("authorization", "")
    token = None

    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
    else:
        # Fallback: ?token=<jwt> query parameter
        token = websocket.query_params.get("token")

    if not token:
        return False

    try:
        verify_token(token)
        return True
    except Exception:
        return False
```

Remove the old `import base64` and `import secrets` if they are no longer used elsewhere
in the file (check carefully — `uuid` is still used).

#### 6. Update `docs/` — add `docs/auth.md` (new file)

Document the auth flow:
```markdown
# Authentication

## Overview
The API uses JWT Bearer tokens for authentication.

## Getting a Token
POST /auth/token with HTTP Basic Auth credentials:
```
curl -u admin:password -X POST http://localhost:8000/auth/token
```
Response:
```json
{"access_token": "<jwt>", "token_type": "bearer", "expires_in": 3600}
```

## Using a Token
Include in all API requests:
```
Authorization: Bearer <jwt>
```

## WebSocket Authentication
Pass the token as a query parameter:
```
ws://localhost:8000/ws/executors?token=<jwt>
```
Or as an `Authorization: Bearer <jwt>` header during the handshake.

## Token Expiry
Tokens expire after `JWT_EXPIRE_MINUTES` (default 60 minutes).
Re-issue a new token by calling `POST /auth/token` again.
```

### Acceptance criteria:
- `POST /auth/token` with correct Basic Auth returns a JWT with `"token_type": "bearer"`.
- `GET /bot-orchestration/status` without a token returns `HTTP 401`.
- `GET /bot-orchestration/status` with `Authorization: Bearer <valid_jwt>` returns `HTTP 200`.
- An expired or tampered token returns `HTTP 401` with `"Token has expired"` or `"Invalid token"`.
- `GET /health` still returns `HTTP 200` with no auth header (it must remain public).
- WebSocket connection to `/ws/executors?token=<valid_jwt>` is accepted.
- WebSocket connection without a token is rejected with code `4001`.
- `debug_mode=True` in `.env` bypasses all auth (existing behavior preserved).
- Do NOT change the `auth_user` function name — other parts of the codebase reference it.
- Use `logger = logging.getLogger(__name__)` in all new files.
- Follow existing try/except patterns from the codebase.

Read `main.py`, `config.py`, `routers/websocket.py`, and the existing `routers/` directory
in full before making any changes. Check `environment.yml` for PyJWT before adding it.
