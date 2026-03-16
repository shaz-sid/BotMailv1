import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import get_db
from models import Campaign, CampaignStatus, Contact, Email, EmailStatus
from services.campaign_service import run_campaign, CampaignReport
from services.gemini_service import generate_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["Campaigns"])

DB = Annotated[Session, Depends(get_db)]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class CampaignCreate(BaseModel):
    name:        str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)


class CampaignUpdate(BaseModel):
    name:        str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    status:      CampaignStatus | None = None


class CampaignResponse(BaseModel):
    id:          int
    name:        str
    description: str | None
    status:      CampaignStatus
    total_sent:  int
    open_rate:   float
    reply_rate:  float

    model_config = {"from_attributes": True}


class CampaignListResponse(BaseModel):
    total:     int
    page:      int
    per_page:  int
    campaigns: list[CampaignResponse]


class RunConfig(BaseModel):
    dry_run:         bool      = False
    max_send:        int | None = Field(None, ge=1, le=10_000)
    contact_ids:     list[int] | None = None   # None = all active contacts


class RunResponse(BaseModel):
    campaign_id:   int
    campaign_name: str
    sent:          int
    skipped:       int
    ai_failures:   int
    send_failures: int
    total:         int
    summary:       str
    dry_run:       bool


class PreviewResponse(BaseModel):
    contact_id: int
    contact_email: str
    subject:    str
    body:       str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_campaign_or_404(campaign_id: int, db: Session) -> Campaign:
    campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
    if not campaign:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found.")
    return campaign


def _resolve_contacts(db: Session, contact_ids: list[int] | None) -> list[Contact]:
    """Return active contacts, optionally filtered to a specific ID list."""
    query = db.query(Contact).filter(Contact.is_active == True)
    if contact_ids:
        query = query.filter(Contact.id.in_(contact_ids))
    contacts = query.all()

    if not contacts:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No eligible contacts found for this campaign.",
        )
    return contacts


# ---------------------------------------------------------------------------
# POST /campaigns
# ---------------------------------------------------------------------------

@router.post("/", response_model=CampaignResponse, status_code=status.HTTP_201_CREATED)
def create_campaign(payload: CampaignCreate, db: DB):
    """Create a new campaign in draft status."""
    campaign = Campaign(name=payload.name, description=payload.description)
    db.add(campaign)
    db.commit()
    db.refresh(campaign)

    logger.info("Campaign created | id=%s name=%r", campaign.id, campaign.name)
    return campaign


# ---------------------------------------------------------------------------
# GET /campaigns
# ---------------------------------------------------------------------------

@router.get("/", response_model=CampaignListResponse)
def list_campaigns(
    db:       DB,
    page:     int            = Query(1,  ge=1),
    per_page: int            = Query(20, ge=1, le=100),
    status_filter: CampaignStatus | None = Query(None, alias="status"),
):
    """Paginated campaign list, optionally filtered by status."""
    query = db.query(Campaign)

    if status_filter:
        query = query.filter(Campaign.status == status_filter)

    total    = query.count()
    campaigns = (
        query.order_by(Campaign.created_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    return CampaignListResponse(
        total     = total,
        page      = page,
        per_page  = per_page,
        campaigns = campaigns,
    )


# ---------------------------------------------------------------------------
# GET /campaigns/{campaign_id}
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}", response_model=CampaignResponse)
def get_campaign(campaign_id: int, db: DB):
    return _get_campaign_or_404(campaign_id, db)


# ---------------------------------------------------------------------------
# PATCH /campaigns/{campaign_id}
# ---------------------------------------------------------------------------

@router.patch("/{campaign_id}", response_model=CampaignResponse)
def update_campaign(campaign_id: int, payload: CampaignUpdate, db: DB):
    campaign = _get_campaign_or_404(campaign_id, db)

    if campaign.status == CampaignStatus.archived:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Archived campaigns cannot be modified.",
        )

    updates = payload.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(campaign, field, value)

    db.commit()
    db.refresh(campaign)

    logger.info("Campaign updated | id=%s fields=%s", campaign.id, list(updates.keys()))
    return campaign


# ---------------------------------------------------------------------------
# DELETE /campaigns/{campaign_id}
# ---------------------------------------------------------------------------

@router.delete("/{campaign_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_campaign(campaign_id: int, db: DB):
    campaign = _get_campaign_or_404(campaign_id, db)

    if campaign.status == CampaignStatus.active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete an active campaign. Pause it first.",
        )

    db.delete(campaign)
    db.commit()
    logger.info("Campaign deleted | id=%s", campaign_id)


# ---------------------------------------------------------------------------
# POST /campaigns/{campaign_id}/run
# ---------------------------------------------------------------------------

def _execute_campaign(campaign_id: int, config: RunConfig) -> None:
    """Background task: open its own DB session and run the campaign."""
    from database import get_db_context

    with get_db_context() as db:
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            logger.error("Background task: campaign_id=%s not found.", campaign_id)
            return

        contacts = _resolve_contacts(db, config.contact_ids)

        if not config.dry_run:
            campaign.status = CampaignStatus.active
            db.commit()

        report: CampaignReport = run_campaign(
            campaign  = campaign,
            contacts  = contacts,
            db        = db,
            dry_run   = config.dry_run,
            max_send  = config.max_send,
        )

        if not config.dry_run:
            campaign.status = CampaignStatus.completed
            db.commit()

        logger.info(report.summary)


@router.post("/{campaign_id}/run", response_model=RunResponse)
def run_campaign_route(
    campaign_id:      int,
    config:           RunConfig,
    background_tasks: BackgroundTasks,
    db:               DB,
):
    """
    Trigger a campaign send.

    - Validates the campaign and contacts synchronously.
    - Hands off the actual sending to a background task so the HTTP
      response returns immediately (important for large contact lists).
    - Pass `dry_run=true` to preview without sending.
    """
    campaign = _get_campaign_or_404(campaign_id, db)

    if campaign.status == CampaignStatus.archived:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Archived campaigns cannot be run.",
        )
    if campaign.status == CampaignStatus.active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Campaign is already running.",
        )

    contacts = _resolve_contacts(db, config.contact_ids)

    logger.info(
        "Queueing campaign | id=%s contacts=%d dry_run=%s",
        campaign_id, len(contacts), config.dry_run,
    )

    background_tasks.add_task(_execute_campaign, campaign_id, config)

    return RunResponse(
        campaign_id   = campaign.id,
        campaign_name = campaign.name,
        sent          = 0,
        skipped       = 0,
        ai_failures   = 0,
        send_failures = 0,
        total         = len(contacts),
        summary       = f"Campaign queued for {len(contacts)} contact(s).",
        dry_run       = config.dry_run,
    )


# ---------------------------------------------------------------------------
# POST /campaigns/{campaign_id}/preview
# ---------------------------------------------------------------------------

@router.post("/{campaign_id}/preview", response_model=PreviewResponse)
def preview_email(campaign_id: int, contact_id: int, db: DB):
    """
    Generate (but do NOT send) a personalised email for one contact.
    Useful for reviewing copy before launching a campaign.
    """
    campaign = _get_campaign_or_404(campaign_id, db)
    contact  = db.query(Contact).filter(
        Contact.id == contact_id,
        Contact.is_active == True,
    ).first()

    if not contact:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contact not found.")

    try:
        generated = generate_email(contact, campaign)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI generation failed: {exc}",
        )

    return PreviewResponse(
        contact_id    = contact.id,
        contact_email = contact.email,
        subject       = generated.subject,
        body          = generated.body,
    )


# ---------------------------------------------------------------------------
# GET /campaigns/{campaign_id}/stats
# ---------------------------------------------------------------------------

class CampaignStats(BaseModel):
    campaign_id:   int
    total_sent:    int
    total_failed:  int
    total_opened:  int
    total_replied: int
    total_clicked: int
    open_rate:     float
    reply_rate:    float
    click_rate:    float


@router.get("/{campaign_id}/stats", response_model=CampaignStats)
def campaign_stats(campaign_id: int, db: DB):
    """Live engagement stats for a campaign."""
    _get_campaign_or_404(campaign_id, db)

    emails = db.query(Email).filter(Email.campaign_id == campaign_id).all()
    sent   = [e for e in emails if e.status == EmailStatus.sent]
    n_sent = len(sent)

    def rate(numerator: int) -> float:
        return round(numerator / n_sent, 4) if n_sent else 0.0

    n_opened  = sum(1 for e in sent if e.opened)
    n_replied = sum(1 for e in sent if e.replied)
    n_clicked = sum(1 for e in sent if e.clicked)

    return CampaignStats(
        campaign_id   = campaign_id,
        total_sent    = n_sent,
        total_failed  = sum(1 for e in emails if e.status == EmailStatus.failed),
        total_opened  = n_opened,
        total_replied = n_replied,
        total_clicked = n_clicked,
        open_rate     = rate(n_opened),
        reply_rate    = rate(n_replied),
        click_rate    = rate(n_clicked),
    )