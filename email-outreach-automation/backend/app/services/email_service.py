import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import resend
from resend.exceptions import ResendError

from config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client setup
# ---------------------------------------------------------------------------

resend.api_key = settings.RESEND_API_KEY

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class EmailReceipt:
    message_id: str
    to: str
    subject: str
    sent_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    provider_response: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RETRY_STATUSES = {429, 500, 502, 503, 504}
_MAX_RETRIES    = 3
_BACKOFF_BASE   = 2          # seconds — doubles each retry (2s, 4s, 8s)


def _validate_email(address: str) -> None:
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    if not re.match(pattern, address):
        raise ValueError(f"Invalid email address: {address!r}")


def _plain_from_html(html: str) -> str:
    """Minimal HTML → plain-text fallback for multipart emails."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _send_with_retry(payload: dict[str, Any]) -> dict[str, Any]:
    """Call Resend with exponential back-off on transient errors."""
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return resend.Emails.send(payload)

        except ResendError as exc:
            status = getattr(exc, "status_code", None)

            if status not in _RETRY_STATUSES:
                raise                          # non-retryable (4xx auth / bad request)

            last_exc = exc
            wait = _BACKOFF_BASE ** attempt
            logger.warning(
                "Resend transient error (attempt %d/%d, status=%s) — retrying in %ds: %s",
                attempt, _MAX_RETRIES, status, wait, exc,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Email send failed after {_MAX_RETRIES} retries."
    ) from last_exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_email(
    to: str,
    subject: str,
    body: str,                         # HTML
    *,
    from_address: str | None = None,
    reply_to: str | None = None,
    tags: list[dict[str, str]] | None = None,
) -> EmailReceipt:
    """
    Send a single transactional email via Resend.

    Args:
        to:           Recipient email address.
        subject:      Email subject line.
        body:         HTML email body.
        from_address: Override the default sender (useful for multi-brand setups).
        reply_to:     Reply-To header address.
        tags:         Resend tags for filtering in the dashboard, e.g.
                      [{"name": "campaign", "value": "launch-2025"}]

    Returns:
        EmailReceipt with the provider message ID and metadata.

    Raises:
        ValueError:   On invalid addresses or missing required fields.
        RuntimeError: If the send fails after retries.
    """
    # --- validation ---
    _validate_email(to)
    if not subject.strip():
        raise ValueError("Email subject must not be empty.")
    if not body.strip():
        raise ValueError("Email body must not be empty.")

    sender = from_address or settings.EMAIL_FROM_ADDRESS
    _validate_email(sender)

    # --- payload ---
    payload: dict[str, Any] = {
        "from":    sender,
        "to":      [to],
        "subject": subject,
        "html":    body,
        "text":    _plain_from_html(body),     # multipart: better deliverability
    }

    if reply_to:
        _validate_email(reply_to)
        payload["reply_to"] = reply_to

    if tags:
        payload["tags"] = tags

    logger.info("Sending email | to=%s subject=%r from=%s", to, subject, sender)

    # --- send ---
    response = _send_with_retry(payload)

    message_id = response.get("id", "")
    logger.info("Email sent | message_id=%s to=%s", message_id, to)

    return EmailReceipt(
        message_id=message_id,
        to=to,
        subject=subject,
        provider_response=response,
    )


def send_bulk(
    recipients: list[dict[str, str]],     # [{"to": ..., "subject": ..., "body": ...}, ...]
    *,
    from_address: str | None = None,
    reply_to: str | None = None,
    tags: list[dict[str, str]] | None = None,
    delay_ms: int = 100,
) -> list[EmailReceipt]:
    """
    Send to multiple recipients individually (preserves personalisation).

    Args:
        recipients: List of dicts with keys ``to``, ``subject``, ``body``.
        delay_ms:   Milliseconds to wait between sends to stay within rate limits.

    Returns:
        List of EmailReceipt for every successful send.
        Failed sends are logged and skipped — they do NOT abort the batch.
    """
    if not recipients:
        raise ValueError("Recipient list must not be empty.")

    receipts: list[EmailReceipt] = []
    failed:   list[str]          = []

    for i, r in enumerate(recipients):
        to      = r.get("to", "")
        subject = r.get("subject", "")
        body    = r.get("body", "")

        try:
            receipt = send_email(
                to=to,
                subject=subject,
                body=body,
                from_address=from_address,
                reply_to=reply_to,
                tags=tags,
            )
            receipts.append(receipt)

        except (ValueError, RuntimeError) as exc:
            logger.error("Bulk send failed for %r (item %d): %s", to, i, exc)
            failed.append(to)

        if delay_ms and i < len(recipients) - 1:
            time.sleep(delay_ms / 1000)

    if failed:
        logger.warning("Bulk send completed with %d failure(s): %s", len(failed), failed)

    return receipts