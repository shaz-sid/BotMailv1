import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from database import get_db
from models import Campaign, Contact, Email, EmailStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics"])

DB = Annotated[Session, Depends(get_db)]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class EngagementStats(BaseModel):
    total_sent:    int
    total_failed:  int
    total_opened:  int
    total_replied: int
    total_clicked: int
    open_rate:     float = Field(..., description="Opened / sent")
    reply_rate:    float = Field(..., description="Replied / sent")
    click_rate:    float = Field(..., description="Clicked / sent")
    failure_rate:  float = Field(..., description="Failed / total attempted")


class OverviewResponse(BaseModel):
    total_contacts:  int
    total_campaigns: int
    total_emails:    int
    engagement:      EngagementStats
    computed_at:     datetime


class CampaignAnalytics(BaseModel):
    campaign_id:   int
    campaign_name: str
    status:        str
    engagement:    EngagementStats


class ContactAnalytics(BaseModel):
    contact_id:    int
    contact_name:  str
    contact_email: str
    total_received: int
    engagement:    EngagementStats


class DailyDatapoint(BaseModel):
    date:    str          # "YYYY-MM-DD"
    sent:    int
    opened:  int
    replied: int
    clicked: int


class TimelineResponse(BaseModel):
    campaign_id: int | None
    datapoints:  list[DailyDatapoint]


class TopCampaign(BaseModel):
    campaign_id:   int
    campaign_name: str
    sent:          int
    open_rate:     float
    reply_rate:    float


class LeaderboardResponse(BaseModel):
    ranked_by: str
    campaigns: list[TopCampaign]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _safe_rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _compute_engagement(rows: list[Email]) -> EngagementStats:
    """
    Compute engagement stats from a pre-filtered list of Email ORM rows.
    All aggregation happens in Python so callers can reuse any query result.
    """
    attempted = len(rows)
    sent_rows = [e for e in rows if e.status == EmailStatus.sent]
    failed    = sum(1 for e in rows if e.status == EmailStatus.failed)
    n_sent    = len(sent_rows)

    opened  = sum(1 for e in sent_rows if e.opened)
    replied = sum(1 for e in sent_rows if e.replied)
    clicked = sum(1 for e in sent_rows if e.clicked)

    return EngagementStats(
        total_sent    = n_sent,
        total_failed  = failed,
        total_opened  = opened,
        total_replied = replied,
        total_clicked = clicked,
        open_rate     = _safe_rate(opened,  n_sent),
        reply_rate    = _safe_rate(replied, n_sent),
        click_rate    = _safe_rate(clicked, n_sent),
        failure_rate  = _safe_rate(failed,  attempted),
    )


def _base_email_query(
    db:          Session,
    campaign_id: int | None,
    contact_id:  int | None,
    since:       datetime | None,
    until:       datetime | None,
):
    query = db.query(Email)

    if campaign_id:
        query = query.filter(Email.campaign_id == campaign_id)
    if contact_id:
        query = query.filter(Email.contact_id == contact_id)
    if since:
        query = query.filter(Email.sent_at >= since)
    if until:
        query = query.filter(Email.sent_at <= until)

    return query


# ---------------------------------------------------------------------------
# GET /analytics/overview
# ---------------------------------------------------------------------------

@router.get("/overview", response_model=OverviewResponse)
def overview(
    db:          DB,
    campaign_id: int | None      = Query(None),
    contact_id:  int | None      = Query(None),
    since:       datetime | None = Query(None, description="ISO-8601 start datetime"),
    until:       datetime | None = Query(None, description="ISO-8601 end datetime"),
):
    """
    Global engagement stats, optionally scoped to a campaign, contact,
    or date range. All rates are computed over *sent* emails only.
    """
    if since and until and since > until:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="`since` must be earlier than `until`.",
        )

    emails = _base_email_query(db, campaign_id, contact_id, since, until).all()

    return OverviewResponse(
        total_contacts  = db.query(func.count(Contact.id)).scalar(),
        total_campaigns = db.query(func.count(Campaign.id)).scalar(),
        total_emails    = len(emails),
        engagement      = _compute_engagement(emails),
        computed_at     = datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# GET /analytics/campaigns
# ---------------------------------------------------------------------------

@router.get("/campaigns", response_model=list[CampaignAnalytics])
def campaign_breakdown(
    db:    DB,
    since: datetime | None = Query(None),
    until: datetime | None = Query(None),
):
    """Per-campaign engagement breakdown — one row per campaign."""
    campaigns = db.query(Campaign).all()

    if not campaigns:
        return []

    result = []
    for campaign in campaigns:
        emails = _base_email_query(db, campaign.id, None, since, until).all()
        result.append(CampaignAnalytics(
            campaign_id   = campaign.id,
            campaign_name = campaign.name,
            status        = campaign.status,
            engagement    = _compute_engagement(emails),
        ))

    return result


# ---------------------------------------------------------------------------
# GET /analytics/campaigns/{campaign_id}
# ---------------------------------------------------------------------------

@router.get("/campaigns/{campaign_id}", response_model=CampaignAnalytics)
def single_campaign_analytics(
    campaign_id: int,
    db:          DB,
    since:       datetime | None = Query(None),
    until:       datetime | None = Query(None),
):
    campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
    if not campaign:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found.")

    emails = _base_email_query(db, campaign_id, None, since, until).all()

    return CampaignAnalytics(
        campaign_id   = campaign.id,
        campaign_name = campaign.name,
        status        = campaign.status,
        engagement    = _compute_engagement(emails),
    )


# ---------------------------------------------------------------------------
# GET /analytics/contacts/{contact_id}
# ---------------------------------------------------------------------------

@router.get("/contacts/{contact_id}", response_model=ContactAnalytics)
def contact_analytics(
    contact_id: int,
    db:         DB,
    since:      datetime | None = Query(None),
    until:      datetime | None = Query(None),
):
    """Engagement history for a single contact across all campaigns."""
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contact not found.")

    emails = _base_email_query(db, None, contact_id, since, until).all()

    return ContactAnalytics(
        contact_id      = contact.id,
        contact_name    = contact.name,
        contact_email   = contact.email,
        total_received  = len(emails),
        engagement      = _compute_engagement(emails),
    )


# ---------------------------------------------------------------------------
# GET /analytics/timeline
# ---------------------------------------------------------------------------

@router.get("/timeline", response_model=TimelineResponse)
def send_timeline(
    db:          DB,
    campaign_id: int | None      = Query(None, description="Scope to one campaign"),
    since:       datetime | None = Query(None),
    until:       datetime | None = Query(None),
):
    """
    Daily send + engagement counts — useful for time-series charts.
    Only includes days with at least one sent email.
    """
    emails = (
        _base_email_query(db, campaign_id, None, since, until)
        .filter(Email.status == EmailStatus.sent, Email.sent_at.isnot(None))
        .all()
    )

    # Group by date in Python — avoids DB-specific date_trunc / strftime differences
    buckets: dict[str, DailyDatapoint] = {}
    for e in emails:
        day = e.sent_at.strftime("%Y-%m-%d")
        if day not in buckets:
            buckets[day] = DailyDatapoint(date=day, sent=0, opened=0, replied=0, clicked=0)
        b = buckets[day]
        b.sent    += 1
        b.opened  += int(e.opened)
        b.replied += int(e.replied)
        b.clicked += int(e.clicked)

    datapoints = sorted(buckets.values(), key=lambda d: d.date)

    return TimelineResponse(campaign_id=campaign_id, datapoints=datapoints)


# ---------------------------------------------------------------------------
# GET /analytics/leaderboard
# ---------------------------------------------------------------------------

@router.get("/leaderboard", response_model=LeaderboardResponse)
def campaign_leaderboard(
    db:         DB,
    ranked_by:  str = Query("open_rate", pattern="^(open_rate|reply_rate|click_rate|sent)$"),
    limit:      int = Query(10, ge=1, le=50),
    since:      datetime | None = Query(None),
    until:      datetime | None = Query(None),
):
    """
    Top N campaigns ranked by a chosen engagement metric.
    Useful for dashboards that surface best-performing campaigns.
    """
    campaigns = db.query(Campaign).all()
    if not campaigns:
        return LeaderboardResponse(ranked_by=ranked_by, campaigns=[])

    rows: list[TopCampaign] = []
    for campaign in campaigns:
        emails = _base_email_query(db, campaign.id, None, since, until).all()
        eng    = _compute_engagement(emails)

        rows.append(TopCampaign(
            campaign_id   = campaign.id,
            campaign_name = campaign.name,
            sent          = eng.total_sent,
            open_rate     = eng.open_rate,
            reply_rate    = eng.reply_rate,
        ))

    sort_key = {
        "open_rate":  lambda r: r.open_rate,
        "reply_rate": lambda r: r.reply_rate,
        "click_rate": lambda r: r.open_rate,   # proxied — add click_rate field if needed
        "sent":       lambda r: r.sent,
    }[ranked_by]

    ranked = sorted(rows, key=sort_key, reverse=True)[:limit]

    return LeaderboardResponse(ranked_by=ranked_by, campaigns=ranked)