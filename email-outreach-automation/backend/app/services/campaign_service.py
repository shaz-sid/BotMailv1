import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy.orm import Session

from models import Email, EmailStatus
from services.email_service import send_email, EmailReceipt
from services.gemini_service import generate_email, GeneratedEmail

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums & dataclasses
# ---------------------------------------------------------------------------

class ContactResult(str, Enum):
    success    = "success"
    skipped    = "skipped"
    ai_failed  = "ai_failed"
    send_failed = "send_failed"


@dataclass
class ContactOutcome:
    contact_id:  int
    email:       str
    result:      ContactResult
    message_id:  str | None = None      # Resend message ID on success
    error:       str | None = None


@dataclass
class CampaignReport:
    campaign_id:  int
    campaign_name: str
    started_at:   datetime
    finished_at:  datetime | None = None
    outcomes:     list[ContactOutcome] = field(default_factory=list)

    # --- computed stats ---
    @property
    def total(self) -> int:
        return len(self.outcomes)

    @property
    def sent(self) -> int:
        return self._count(ContactResult.success)

    @property
    def skipped(self) -> int:
        return self._count(ContactResult.skipped)

    @property
    def ai_failures(self) -> int:
        return self._count(ContactResult.ai_failed)

    @property
    def send_failures(self) -> int:
        return self._count(ContactResult.send_failed)

    def _count(self, result: ContactResult) -> int:
        return sum(1 for o in self.outcomes if o.result == result)

    @property
    def summary(self) -> str:
        duration = (
            (self.finished_at - self.started_at).total_seconds()
            if self.finished_at else 0
        )
        return (
            f"Campaign '{self.campaign_name}' finished in {duration:.1f}s — "
            f"{self.sent} sent, {self.skipped} skipped, "
            f"{self.ai_failures} AI errors, {self.send_failures} send errors "
            f"out of {self.total} contacts."
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_eligible(contact, already_emailed: set[str]) -> tuple[bool, str]:
    """Return (eligible, reason) for a contact."""
    if not contact.is_active:
        return False, "contact is inactive / opted out"
    if not contact.email:
        return False, "contact has no email address"
    if contact.email in already_emailed:
        return False, "already emailed in this run"
    return True, ""


def _persist_email(
    db:       Session,
    contact,
    campaign,
    generated: GeneratedEmail,
    receipt:   EmailReceipt | None,
    status:    EmailStatus,
    error_msg: str | None = None,
) -> None:
    """Write or update the Email row for audit / analytics."""
    email_row = Email(
        contact_id  = contact.id,
        campaign_id = campaign.id,
        subject     = generated.subject,
        body        = generated.body,
        status      = status,
        error_msg   = error_msg,
        sent_at     = receipt.sent_at if receipt else None,
    )
    db.add(email_row)
    db.flush()          # get the PK without committing


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_campaign(
    campaign,
    contacts:      list,
    db:            Session,
    *,
    dry_run:       bool = False,
    max_send:      int  | None = None,
) -> CampaignReport:
    """
    Generate and send personalised emails for every eligible contact.

    Args:
        campaign:   Campaign ORM instance.
        contacts:   List of Contact ORM instances to target.
        db:         Active SQLAlchemy session (caller owns the transaction).
        dry_run:    If True, generate emails but do NOT send — useful for previewing.
        max_send:   Hard cap on sends per run (safety valve for large campaigns).

    Returns:
        CampaignReport with per-contact outcomes and aggregate stats.
    """
    report = CampaignReport(
        campaign_id   = campaign.id,
        campaign_name = campaign.name,
        started_at    = datetime.now(timezone.utc),
    )

    logger.info(
        "Starting campaign | id=%s name=%r contacts=%d dry_run=%s",
        campaign.id, campaign.name, len(contacts), dry_run,
    )

    already_emailed: set[str] = set()
    sends_this_run  = 0

    for contact in contacts:

        # --- rate / safety cap ---
        if max_send is not None and sends_this_run >= max_send:
            logger.warning("Reached max_send=%d — stopping early.", max_send)
            break

        # --- eligibility ---
        eligible, reason = _is_eligible(contact, already_emailed)
        if not eligible:
            logger.info("Skipping contact_id=%s — %s.", contact.id, reason)
            report.outcomes.append(ContactOutcome(
                contact_id = contact.id,
                email      = contact.email or "",
                result     = ContactResult.skipped,
                error      = reason,
            ))
            continue

        # --- AI generation ---
        try:
            generated: GeneratedEmail = generate_email(contact, campaign)
        except (ValueError, RuntimeError) as exc:
            logger.error("AI generation failed | contact_id=%s: %s", contact.id, exc)
            report.outcomes.append(ContactOutcome(
                contact_id = contact.id,
                email      = contact.email,
                result     = ContactResult.ai_failed,
                error      = str(exc),
            ))
            _persist_email(db, contact, campaign, GeneratedEmail(
                subject="", body="", raw_response=""
            ), None, EmailStatus.failed, str(exc))
            continue

        # --- dry-run short-circuit ---
        if dry_run:
            logger.info(
                "[DRY RUN] Would send | contact_id=%s subject=%r",
                contact.id, generated.subject,
            )
            report.outcomes.append(ContactOutcome(
                contact_id = contact.id,
                email      = contact.email,
                result     = ContactResult.skipped,
                error      = "dry_run=True",
            ))
            continue

        # --- send ---
        try:
            receipt: EmailReceipt = send_email(
                to      = contact.email,
                subject = generated.subject,
                body    = generated.body,
                tags    = [
                    {"name": "campaign_id", "value": str(campaign.id)},
                    {"name": "contact_id",  "value": str(contact.id)},
                ],
            )
        except (ValueError, RuntimeError) as exc:
            logger.error("Send failed | contact_id=%s: %s", contact.id, exc)
            report.outcomes.append(ContactOutcome(
                contact_id = contact.id,
                email      = contact.email,
                result     = ContactResult.send_failed,
                error      = str(exc),
            ))
            _persist_email(db, contact, campaign, generated, None, EmailStatus.failed, str(exc))
            continue

        # --- success ---
        already_emailed.add(contact.email)
        sends_this_run += 1

        _persist_email(db, contact, campaign, generated, receipt, EmailStatus.sent)

        report.outcomes.append(ContactOutcome(
            contact_id = contact.id,
            email      = contact.email,
            result     = ContactResult.success,
            message_id = receipt.message_id,
        ))

        logger.info(
            "Sent | contact_id=%s message_id=%s subject=%r",
            contact.id, receipt.message_id, generated.subject,
        )

    # --- finalise ---
    report.finished_at = datetime.now(timezone.utc)
    logger.info(report.summary)

    return report