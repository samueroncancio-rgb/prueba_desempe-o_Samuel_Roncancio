from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, date

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str

class UserProfile(BaseModel):
    username: str
    role: str

class KPIResponse(BaseModel):
    total_events_24h: int
    max_magnitude: float
    max_mag_place: str
    avg_magnitude: float
    tsunami_alerts: int
    quality_score: float
    last_updated: datetime

class EarthquakeEventItem(BaseModel):
    event_id: str
    mag: float
    mag_type: Optional[str] = None
    place_sanitized: str
    event_time: datetime
    updated_time: datetime
    longitude: float
    latitude: float
    depth_km: float
    tsunami_alert: bool
    significance: int
    status: str
    is_auto_healed: bool
    healing_action: Optional[str] = None

class PaginatedEventsResponse(BaseModel):
    items: List[EarthquakeEventItem]
    total: int
    page: int
    limit: int
    pages: int

class TimeseriesPoint(BaseModel):
    time_bucket: str
    event_count: int
    avg_magnitude: float
    max_magnitude: float

class DailyReportItem(BaseModel):
    report_id: str
    report_date: date
    title: str
    summary_narrative: str
    calculated_metrics: Dict[str, Any]
    llm_metrics_verified: bool
    is_fallback: bool
    created_at: datetime

class QuarantineEventItem(BaseModel):
    quarantine_id: str
    raw_event_id: Optional[str]
    rejected_at: datetime
    rejection_reason: str
    rule_violated: str
    raw_payload: Optional[Dict[str, Any]]

class BackfillRequest(BaseModel):
    days: int = Field(default=30, ge=1, le=30)
    min_magnitude: float = Field(default=2.5, ge=0.0)

class RegenerateReportRequest(BaseModel):
    target_date: Optional[str] = None

class ChatMessageRequest(BaseModel):
    message: str = Field(..., min_length=2, max_length=500)

class ChatMessageResponse(BaseModel):
    reply: str
    is_domain_pertinent: bool
    sources: List[Dict[str, Any]]
    model: str
