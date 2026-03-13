import logging
import re
from dataclasses import dataclass

import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError

from config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client setup
# ---------------------------------------------------------------------------

genai.configure(api_key=settings.GEMINI_API_KEY)

_model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config=genai.GenerationConfig(
        temperature=0.7,       # balanced creativity vs consistency
        top_p=0.9,
        max_output_tokens=1024,
    ),
    safety_settings={
        "HARASSMENT":        "BLOCK_MEDIUM_AND_ABOVE",
        "HATE_SPEECH":       "BLOCK_MEDIUM_AND_ABOVE",
        "SEXUALLY_EXPLICIT": "BLOCK_MEDIUM_AND_ABOVE",
        "DANGEROUS_CONTENT": "BLOCK_MEDIUM_AND_ABOVE",
    },
)

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class GeneratedEmail:
    subject: str
    body: str
    raw_response: str


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert B2B outreach copywriter. Your emails are concise, \
human, and always relevant to the recipient's role and company. \
Never use hollow filler phrases like "I hope this email finds you well."
"""

def _build_prompt(contact, campaign: str) -> str:
    parts = [
        "Write a personalized outreach email using the details below.",
        "",
        "## Recipient",
        f"- Name:    {contact.name}",
        f"- Role:    {contact.role or 'Unknown'}",
        f"- Company: {contact.company or 'Unknown'}",
    ]
    if getattr(contact, "linkedin", None):
        parts.append(f"- LinkedIn: {contact.linkedin}")

    parts += [
        "",
        "## Campaign goal",
        campaign.description or "(no description provided)",
        "",
        "## Output format",
        "Return ONLY the following two sections — no extra commentary:",
        "SUBJECT: <one concise subject line>",
        "BODY:",
        "<email body, 3-5 short paragraphs, plain text>",
    ]
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_response(text: str) -> tuple[str, str]:
    """Extract subject and body from the model's structured response."""
    subject_match = re.search(r"^SUBJECT:\s*(.+)$", text, re.MULTILINE | re.IGNORECASE)
    body_match    = re.search(r"^BODY:\s*\n([\s\S]+)", text, re.MULTILINE | re.IGNORECASE)

    subject = subject_match.group(1).strip() if subject_match else ""
    body    = body_match.group(1).strip()    if body_match    else text.strip()

    return subject, body


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_email(contact, campaign) -> GeneratedEmail:
    """
    Generate a personalised outreach email for `contact` within `campaign`.

    Raises:
        ValueError: if required contact / campaign fields are missing.
        RuntimeError: if the Gemini API call fails after retries.
    """
    # --- input validation ---
    if not contact.name or not contact.email:
        raise ValueError(f"Contact {contact.id} is missing name or email.")
    if not campaign.description:
        raise ValueError(f"Campaign {campaign.id} has no description to base the email on.")

    prompt = _build_prompt(contact, campaign)

    logger.info(
        "Generating email | contact_id=%s campaign_id=%s",
        contact.id, campaign.id,
    )

    # --- API call ---
    try:
        response = _model.generate_content(
            contents=[
                {"role": "user", "parts": [_SYSTEM_PROMPT]},
                {"role": "model", "parts": ["Understood. Send me the details."]},
                {"role": "user", "parts": [prompt]},
            ]
        )
    except GoogleAPIError as exc:
        logger.error("Gemini API error for contact_id=%s: %s", contact.id, exc)
        raise RuntimeError(f"Email generation failed: {exc}") from exc

    # --- safety / finish-reason checks ---
    candidate = response.candidates[0]
    finish_reason = candidate.finish_reason.name  # "STOP", "SAFETY", "MAX_TOKENS", …

    if finish_reason == "SAFETY":
        logger.warning("Response blocked by safety filters | contact_id=%s", contact.id)
        raise RuntimeError("Email generation was blocked by Gemini safety filters.")

    if finish_reason == "MAX_TOKENS":
        logger.warning("Response truncated (MAX_TOKENS) | contact_id=%s", contact.id)

    raw = response.text
    subject, body = _parse_response(raw)

    if not body:
        raise RuntimeError("Gemini returned an empty email body.")

    logger.info(
        "Email generated successfully | contact_id=%s subject=%r",
        contact.id, subject,
    )

    return GeneratedEmail(subject=subject, body=body, raw_response=raw)


def generate_subject_variants(subject: str, n: int = 3) -> list[str]:
    """Return `n` alternative subject lines for A/B testing."""
    if n < 1 or n > 10:
        raise ValueError("`n` must be between 1 and 10.")

    prompt = (
        f"Given this email subject line:\n\n{subject!r}\n\n"
        f"Write {n} alternative subject lines for A/B testing. "
        "Return them as a plain numbered list, nothing else."
    )

    try:
        response = _model.generate_content(prompt)
    except GoogleAPIError as exc:
        raise RuntimeError(f"Subject variant generation failed: {exc}") from exc

    lines = [
        re.sub(r"^\d+[\.\)]\s*", "", ln).strip()
        for ln in response.text.splitlines()
        if ln.strip()
    ]
    return lines[:n]