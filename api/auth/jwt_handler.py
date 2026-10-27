import datetime
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from passlib.context import CryptContext

from airflow.include.config import config
from api.schemas.pydantic_models import UserProfile

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security_bearer = HTTPBearer(auto_error=False)

def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.datetime.now(datetime.timezone.utc) + (
        expires_delta if expires_delta else datetime.timedelta(minutes=config.jwt_access_token_expire_minutes)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, config.jwt_secret_key, algorithm=config.jwt_algorithm)

def verify_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, config.jwt_secret_key, algorithms=[config.jwt_algorithm])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None or role is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token payload invalid",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return {"username": username, "role": role}
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials or token expired",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)) -> UserProfile:
    if not credentials or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Autenticación requerida. Token no provisto.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token_data = verify_token(credentials.credentials)
    return UserProfile(username=token_data["username"], role=token_data["role"])

def require_viewer(current_user: UserProfile = Depends(get_current_user)) -> UserProfile:
    if current_user.role not in ["viewer", "admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso restringido: requiere rol viewer o admin."
        )
    return current_user

def require_admin(current_user: UserProfile = Depends(get_current_user)) -> UserProfile:
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso restringido: acción exclusiva de administrador."
        )
    return current_user
