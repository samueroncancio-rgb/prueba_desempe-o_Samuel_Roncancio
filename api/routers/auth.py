from fastapi import APIRouter, HTTPException, status, Depends
from airflow.include.config import config
from api.schemas.pydantic_models import LoginRequest, TokenResponse, UserProfile
from api.auth.jwt_handler import create_access_token, get_current_user

router = APIRouter(prefix="/auth", tags=["Authentication"])

@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest):
    username = payload.username.strip()
    password = payload.password.strip()

    # Authenticate Admin
    if username == config.admin_user and password == config.admin_password:
        token = create_access_token({"sub": username, "role": "admin"})
        return TokenResponse(access_token=token, role="admin", username=username)

    # Authenticate Viewer
    if username == config.viewer_user and password == config.viewer_password:
        token = create_access_token({"sub": username, "role": "viewer"})
        return TokenResponse(access_token=token, role="viewer", username=username)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciales incorrectas. Verifique usuario y contraseña.",
        headers={"WWW-Authenticate": "Bearer"},
    )

@router.get("/me", response_model=UserProfile)
def get_me(user: UserProfile = Depends(get_current_user)):
    return user
