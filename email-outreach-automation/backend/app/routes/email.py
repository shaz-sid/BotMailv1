import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session, joinedload
from datetime import datetime

from database import get_db
from models import Contact, Campaign, Email, EmailStatus
from services.email_service import send_email as resend_email
from services.gemini_service import generate_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/emails", tags=["Emails"])

DB = Annotated[Session, Depends(get_db)]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ContactSummary(BaseModel):
    id:    int
    name:  str
    email: EmailStr

    model_config = {"from_attributes": True}


class CampaignSummary(BaseModel):
    id:   int
    name: str

    model_config = {"from_attributes": True}


class EmailResponse(BaseModel):
    id:          int
    subject:     str | None
    body:        str | None
    status:      EmailStatus
    sent_at:     datetime | None
    opened:      bool
    replied:     bool
    clicked:     bool
    error_msg:   str | None
    created_at:  datetime
    contact:     ContactSummary | None
    campaign:    CampaignSummary | None

    model_config = {"from_attributes": True}


class EmailListResponse(BaseModel):
    total:    int
    page:     int
    per_page: int
    emails:   list[EmailResponse]


class PreviewRequest(BaseModel):
    contact_id:  int
    campaign_id: int


class PreviewResponse(BaseModel):
    contact_id:    int
    campaign_id:   int
    contact_email: str
    subject:       str
    body:          str


class SendRequest(BaseModel):
    contact_id:  int
    campaign_id: int


class SendResponse(BaseModel):
    email_id:   int
    message_id: str
    to:         str
    subject:    str
    sent_at:    datetime


class RetryResponse(BaseModel):
    email_id:   int
    message_id: str
    sent_at:    datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_email_or_404(email_id: int, db: Session) -> Email:
    email = (
        db.query(Email)
        .options(joinedload(Email.contact), joinedload(Email.campaign))
        .filter(Email.id == email_id)
        .first()
    )
    if not email:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Email not found.")
    return email


def _get_contact_or_404(contact_id: int, db: Session) -> Contact:
    contact = db.query(Contact).filter(
        Contact.id == contact_id,
        Contact.is_active == True,
    ).first()
    if not contact:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Contact not found or is opted out.",
        )
    return contact


def _get_campaign_or_404(campaign_id: int, db: Session) -> Campaign:
    campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
    if not campaign:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found.")
    return campaign


# ---------------------------------------------------------------------------
# GET /emails
# ---------------------------------------------------------------------------

@router.get("/", response_model=EmailListResponse)
def list_emails(
    db:              DB,
    page:            int                = Query(1,    ge=1),
    per_page:        int                = Query(20,   ge=1, le=100),
    campaign_id:     int | None         = Query(None, description="Filter by campaign"),
    contact_id:      int | None         = Query(None, description="Filter by contact"),
    status_filter:   EmailStatus | None = Query(None, alias="status"),
    opened:          bool | None        = Query(None, description="Filter by opened flag"),
    replied:         bool | None        = Query(None, description="Filter by replied flag"),
):
    """
    Paginated email list with flexible filters.
    Eagerly loads contact + campaign to avoid N+1 queries.
    """
    query = (
        db.query(Email)
        .options(joinedload(Email.contact), joinedload(Email.campaign))
    )

    if campaign_id:
        query = query.filter(Email.campaign_id == campaign_id)
    if contact_id:
        query = query.filter(Email.contact_id == contact_id)
    if status_filter:
        query = query.filter(Email.status == status_filter)
    if opened is not None:
        query = query.filter(Email.opened == opened)
    if replied is not None:
        query = query.filter(Email.replied == replied)

    total  = query.count()
    emails = (
        query.order_by(Email.created_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    return EmailListResponse(
        total    = total,
        page     = page,
        per_page = per_page,
        emails   = emails,
    )


# ---------------------------------------------------------------------------
# GET /emails/{email_id}
# ---------------------------------------------------------------------------

@router.get("/{email_id}", response_model=EmailResponse)
def get_email(email_id: int, db: DB):
    """Fetch a single email with its contact and campaign."""
    return _get_email_or_404(email_id, db)


# ---------------------------------------------------------------------------
# POST /emails/preview
# ---------------------------------------------------------------------------

@router.post("/preview", response_model=PreviewResponse, status_code=status.HTTP_200_OK)
def preview_email(payload: PreviewRequest, db: DB):
    """
    Generate a personalised email via Gemini without sending or persisting it.
    Use this to review copy before triggering a send.
    """
    contact  = _get_contact_or_404(payload.contact_id, db)
    campaign = _get_campaign_or_404(payload.campaign_id, db)

    try:
        generated = generate_email(contact, campaign)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI generation failed: {exc}",
        )

    return PreviewResponse(
        contact_id    = contact.id,
        campaign_id   = campaign.id,
        contact_email = contact.email,
        subject       = generated.subject,
        body          = generated.body,
    )


# ---------------------------------------------------------------------------
# POST /emails/send
# ---------------------------------------------------------------------------

@router.post("/send", response_model=SendResponse, status_code=status.HTTP_201_CREATED)
def send_single_email(payload: SendRequest, db: DB):
    """
    Generate and immediately send one email for a contact + campaign pair.
    Persists an Email row regardless of success or failure.
    """
    contact  = _get_contact_or_404(payload.contact_id, db)
    campaign = _get_campaign_or_404(payload.campaign_id, db)

    # Check for an existing sent email to prevent accidental duplicates
    existing = db.query(Email).filter(
        Email.contact_id  == contact.id,
        Email.campaign_id == campaign.id,
        Email.status      == EmailStatus.sent,
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"An email was already sent to {contact.email!r} for this campaign.",
        )

    # --- generate ---
    try:
        generated = generate_email(contact, campaign)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI generation failed: {exc}",
        )

    # --- send ---
    email_row = Email(
        contact_id  = contact.id,
        campaign_id = campaign.id,
        subject     = generated.subject,
        body        = generated.body,
    )
    db.add(email_row)
    db.flush()

    try:
        receipt = resend_email(
            to      = contact.email,
            subject = generated.subject,
            body    = generated.body,
            tags    = [
                {"name": "campaign_id", "value": str(campaign.id)},
                {"name": "contact_id",  "value": str(contact.id)},
            ],
        )
        email_row.mark_sent()
        logger.info("Email sent | email_id=%s message_id=%s", email_row.id, receipt.message_id)

    except (ValueError, RuntimeError) as exc:
        email_row.mark_failed(str(exc))
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Send failed: {exc}",
        )

    db.commit()

    return SendResponse(
        email_id   = email_row.id,
        message_id = receipt.message_id,
        to         = contact.email,
        subject    = generated.subject,
        sent_at    = receipt.sent_at,
    )


# ---------------------------------------------------------------------------
# POST /emails/{email_id}/retry
# ---------------------------------------------------------------------------

@router.post("/{email_id}/retry", response_model=RetryResponse)
def retry_email(email_id: int, db: DB):
    """
    Re-send a previously failed email without re-generating the copy.
    Only allowed for emails with status `failed` or `bounced`.
    """
    email = _get_email_or_404(email_id, db)

    if email.status not in (EmailStatus.failed, EmailStatus.bounced):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Only failed or bounced emails can be retried. Current status: {email.status}.",
        )

    if not email.contact or not email.contact.is_active:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot retry — contact is inactive or opted out.",
        )

    try:
        receipt = resend_email(
            to      = email.contact.email,
            subject = email.subject,
            body    = email.body,
            tags    = [
                {"name": "campaign_id", "value": str(email.campaign_id)},
                {"name": "contact_id",  "value": str(email.contact_id)},
                {"name": "retry",       "value": "true"},
            ],
        )
    except (ValueError, RuntimeError) as exc:
        email.mark_failed(str(exc))
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Retry send failed: {exc}",
        )

    email.mark_sent()
    db.commit()

    logger.info("Email retried | email_id=%s message_id=%s", email.id, receipt.message_id)

    return RetryResponse(
        email_id   = email.id,
        message_id = receipt.message_id,
        sent_at    = receipt.sent_at,
    )


# ---------------------------------------------------------------------------
# PATCH /emails/{email_id}/track
# ---------------------------------------------------------------------------

class TrackEvent(BaseModel):
    opened:  bool | None = None
    replied: bool | None = None
    clicked: bool | None = None


@router.patch("/{email_id}/track", response_model=EmailResponse)
def track_email_event(email_id: int, payload: TrackEvent, db: DB):
    """
    Update engagement flags (opened / replied / clicked).
    Called by your webhook handler or tracking pixel endpoint.
    """
    email = _get_email_or_404(email_id, db)

    if email.status != EmailStatus.sent:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Engagement can only be tracked on sent emails.",
        )

    updates = payload.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(email, field, value)

    db.commit()
    db.refresh(email)

    logger.info("Email tracked | email_id=%s events=%s", email_id, updates)
    return email