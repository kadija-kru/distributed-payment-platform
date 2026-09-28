from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
from fastapi import Header, HTTPException, status

from shared.config import get_settings

_DEMO_USERS = {"demo": {"password": "demo-password", "subject": "user-demo"}}


def issue_token(username: str, password: str) -> str:
    user = _DEMO_USERS.get(username)
    if not user or user["password"] != password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials")
    now = datetime.now(UTC)
    settings = get_settings()
    return jwt.encode(
        {"sub": user["subject"], "preferred_username": username, "iss": settings.jwt_issuer, "aud": settings.jwt_audience, "iat": now, "exp": now + timedelta(hours=12)},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )


async def require_auth(authorization: str = Header(default="")) -> dict[str, str]:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm], issuer=settings.jwt_issuer, audience=settings.jwt_audience)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token") from exc
    return {"sub": payload["sub"], "username": payload.get("preferred_username", "unknown")}
