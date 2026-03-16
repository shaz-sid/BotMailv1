import logging
from datetime import datetime, timezone
from typing import Any

from celery import Celery
from celery.signals import task_failure, task_postrun, task_prerun
from celery.utils.log import get_task_logger
from kombu import Queue

from config import settings
from database import get_db_context
from models import Campaign, CampaignStatus, Contact, Email, EmailStatus
from services.campaign_service import run_campaign, CampaignReport
from services.email_service import send_email, EmailReceipt
from services.gemini_service import generate_email

logger = get_task_logger(__name__)

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_celery_app() -> Celery:
    app = Celery(
        "email_outreach",
        broker=settings.REDIS_URL,
        backend=settings.REDIS_URL,
    )

    app.conf.update(
        # --- serialisation ---
        task_serializer          = "json",
        result_serializer        = "json",
        accept_content           = ["json"],

        # --- reliability ---
        task_acks_late           = True,       # ack AFTER the task completes, not before
        task_reject_on_worker_lost = True,     # requeue if the worker crashes mid-task
        task_track_started       = True,       # expose "STARTED" state in result backend

        # --- timeouts ---
        task_soft_time_limit     = 300,        # 5 min  → raises SoftTimeLimitExceeded
        task_time_limit          = 360,        # 6 min  → hard kill

        # --- retries ---
        task_max_retries         = 3,

        # --- results ---
        result_expires           = 60 * 60 * 24,   # keep results for 24 h

        # --- routing ---
        task_queues = (
            Queue("default"),
            Queue("emails"),        # high-throughput send tasks
            Queue("campaigns"),     # longer-running campaign orchestration
        ),
        task_default_queue       = "default",
        task_routes = {
            "workers.celery_worker.send_single_email_task": {"queue": "emails"},
            "workers.celery_worker.run_campaign_task":      {"queue": "campaigns"},
            "workers.celery_worker.retry_failed_emails_task": {"queue": "emails"},
        },

        # --- concurrency hint (override via CLI) ---
        worker_prefetch_multiplier = 1,        # fair dispatch for long tasks
    )

    return app


celery = create_celery_app()


# ---------------------------------------------------------------------------
# Lifecycle signals  (structured logging for every task)
# ---------------------------------------------------------------------------

@task_prerun.connect
def on_task_start(task_id: str, task, args, kwargs, **_):
    logger.info("TASK STARTED  | task=%s id=%s args=%s", task.name, task_id, args)


@task_postrun.connect
def on_task_end(task_id: str, task, retval, state, **_):
    logger.info("TASK FINISHED | task=%s id=%s state=%s", task.name, task_id, state)


@task_failure.connect
def on_task_failure(task_id: str, exception, traceback, sender, **_):
    logger.error(
        "TASK FAILED   | task=%s id=%s error=%s",
        sender.name, task_id, exception, exc_info=True,
    )


# ---------------------------------------------------------------------------
# Task 1 — send a single email
# ---------------------------------------------------------------------------

@celery.task(
    bind              = True,
    name              = "workers.celery_worker.send_single_email_task",
    max_retries       = 3,
    default_retry_delay = 60,       # 1 min base; doubled each retry automatically
)
def send_single_email_task(
    self,
    email_id: int,
) -> dict[str, Any]:
    """
    Send one persisted Email row.

    Retries up to 3× on transient errors (network, rate-limit).
    Marks the row `failed` permanently after max retries are exhausted.
    """
    with get_db_context() as db:
        email: Email | None = (
            db.query(Email)
            .filter(Email.id == email_id)
            .first()
        )

        if not email:
            logger.error("Email row not found | email_id=%s", email_id)
            return {"status": "not_found", "email_id": email_id}

        if email.status == EmailStatus.sent:
            logger.warning("Email already sent, skipping | email_id=%s", email_id)
            return {"status": "already_sent", "email_id": email_id}

        if not email.contact or not email.contact.is_active:
            email.mark_failed("Contact is inactive or opted out.")
            return {"status": "skipped", "email_id": email_id, "reason": "contact_inactive"}

        try:
            receipt: EmailReceipt = send_email(
                to      = email.contact.email,
                subject = email.subject,
                body    = email.body,
                tags    = [
                    {"name": "campaign_id", "value": str(email.campaign_id)},
                    {"name": "contact_id",  "value": str(email.contact_id)},
                    {"name": "email_id",    "value": str(email_id)},
                ],
            )

        except (ValueError, RuntimeError) as exc:
            logger.warning(
                "Send failed, scheduling retry | email_id=%s attempt=%s error=%s",
                email_id, self.request.retries, exc,
            )
            try:
                raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))
            except self.MaxRetriesExceededError:
                email.mark_failed(f"Max retries exceeded: {exc}")
                return {"status": "failed", "email_id": email_id, "error": str(exc)}

        email.mark_sent()
        logger.info("Email sent | email_id=%s message_id=%s", email_id, receipt.message_id)

        return {
            "status":     "sent",
            "email_id":   email_id,
            "message_id": receipt.message_id,
            "sent_at":    receipt.sent_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Task 2 — generate + queue sends for an entire campaign
# ---------------------------------------------------------------------------

@celery.task(
    bind              = True,
    name              = "workers.celery_worker.run_campaign_task",
    max_retries       = 1,
    default_retry_delay = 120,
    soft_time_limit   = 600,
)
def run_campaign_task(
    self,
    campaign_id: int,
    contact_ids: list[int] | None = None,
    dry_run:     bool             = False,
    max_send:    int | None       = None,
) -> dict[str, Any]:
    """
    Orchestrate a full campaign send.

    1. Loads campaign + contacts from the DB.
    2. Calls `run_campaign()` which generates emails via Gemini
       and dispatches individual `send_single_email_task` jobs.
    3. Updates campaign status on the way in and out.
    """
    with get_db_context() as db:
        campaign: Campaign | None = (
            db.query(Campaign).filter(Campaign.id == campaign_id).first()
        )

        if not campaign:
            logger.error("Campaign not found | campaign_id=%s", campaign_id)
            return {"status": "not_found", "campaign_id": campaign_id}

        if campaign.status == CampaignStatus.archived:
            return {"status": "skipped", "reason": "campaign_archived"}

        # --- resolve contacts ---
        contact_query = db.query(Contact).filter(Contact.is_active == True)
        if contact_ids:
            contact_query = contact_query.filter(Contact.id.in_(contact_ids))
        contacts = contact_query.all()

        if not contacts:
            logger.warning("No eligible contacts | campaign_id=%s", campaign_id)
            return {"status": "skipped", "reason": "no_eligible_contacts"}

        # --- mark active ---
        if not dry_run:
            campaign.status = CampaignStatus.active
            db.commit()

        logger.info(
            "Campaign starting | id=%s contacts=%d dry_run=%s",
            campaign_id, len(contacts), dry_run,
        )

        try:
            report: CampaignReport = run_campaign(
                campaign = campaign,
                contacts = contacts,
                db       = db,
                dry_run  = dry_run,
                max_send = max_send,
            )
        except Exception as exc:
            campaign.status = CampaignStatus.paused
            db.commit()
            logger.error("Campaign run error | campaign_id=%s: %s", campaign_id, exc)
            raise self.retry(exc=exc)

        # --- mark completed ---
        if not dry_run:
            campaign.status = CampaignStatus.completed
            db.commit()

        logger.info(report.summary)

        return {
            "status":       "completed",
            "campaign_id":  campaign_id,
            "dry_run":      dry_run,
            "sent":         report.sent,
            "skipped":      report.skipped,
            "ai_failures":  report.ai_failures,
            "send_failures": report.send_failures,
            "total":        report.total,
            "summary":      report.summary,
        }


# ---------------------------------------------------------------------------
# Task 3 — retry all failed emails for a campaign
# ---------------------------------------------------------------------------

@celery.task(
    bind        = True,
    name        = "workers.celery_worker.retry_failed_emails_task",
    max_retries = 1,
)
def retry_failed_emails_task(
    self,
    campaign_id: int,
) -> dict[str, Any]:
    """
    Find every `failed` or `bounced` email in a campaign and re-queue
    individual send tasks for each one. Does NOT re-generate copy.
    """
    with get_db_context() as db:
        failed_emails = (
            db.query(Email)
            .filter(
                Email.campaign_id == campaign_id,
                Email.status.in_([EmailStatus.failed, EmailStatus.bounced]),
            )
            .all()
        )

        if not failed_emails:
            logger.info("No failed emails to retry | campaign_id=%s", campaign_id)
            return {"status": "nothing_to_retry", "campaign_id": campaign_id}

        queued = 0
        for email in failed_emails:
            if email.contact and email.contact.is_active:
                send_single_email_task.apply_async(
                    args    = [email.id],
                    queue   = "emails",
                    countdown = queued * 2,   # stagger sends by 2s each
                )
                queued += 1

        logger.info("Retries queued | campaign_id=%s count=%d", campaign_id, queued)
        return {
            "status":      "retries_queued",
            "campaign_id": campaign_id,
            "queued":      queued,
            "skipped":     len(failed_emails) - queued,
        }


# ---------------------------------------------------------------------------
# Task 4 — scheduled nightly cleanup
# ---------------------------------------------------------------------------

@celery.task(name="workers.celery_worker.cleanup_stale_campaigns_task")
def cleanup_stale_campaigns_task() -> dict[str, Any]:
    """
    Nightly task: move campaigns that have been `active` for over 24 hours
    with zero sends into `paused` status so they don't block dashboards.
    """
    from sqlalchemy import func

    cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    with get_db_context() as db:
        stale = (
            db.query(Campaign)
            .filter(
                Campaign.status     == CampaignStatus.active,
                Campaign.created_at <  cutoff,
            )
            .all()
        )

        paused_ids = []
        for campaign in stale:
            sent_count = (
                db.query(func.count(Email.id))
                .filter(
                    Email.campaign_id == campaign.id,
                    Email.status      == EmailStatus.sent,
                )
                .scalar()
            )
            if sent_count == 0:
                campaign.status = CampaignStatus.paused
                paused_ids.append(campaign.id)

        db.commit()

    logger.info("Cleanup complete | stale_campaigns_paused=%s", paused_ids)
    return {"status": "done", "paused": paused_ids}


# ---------------------------------------------------------------------------
# Beat schedule  (requires celery beat running alongside the worker)
# ---------------------------------------------------------------------------

celery.conf.beat_schedule = {
    "nightly-stale-campaign-cleanup": {
        "task":     "workers.celery_worker.cleanup_stale_campaigns_task",
        "schedule": 60 * 60 * 24,      # every 24 hours
    },
}