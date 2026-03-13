import io
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS  = {"name", "email"}
OPTIONAL_COLUMNS  = {"company", "role", "linkedin", "twitter"}
ALL_COLUMNS       = REQUIRED_COLUMNS | OPTIONAL_COLUMNS

MAX_FILE_SIZE_MB  = 10
MAX_ROWS          = 10_000

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ParsedContact:
    name:     str
    email:    str
    company:  str | None = None
    role:     str | None = None
    linkedin: str | None = None
    twitter:  str | None = None


@dataclass
class ParseResult:
    contacts:       list[ParsedContact]
    total_rows:     int
    skipped_rows:   int
    duplicate_rows: int
    errors:         list[str] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return len(self.contacts)

    @property
    def summary(self) -> str:
        return (
            f"Parsed {self.success_count}/{self.total_rows} contacts — "
            f"{self.skipped_rows} invalid, {self.duplicate_rows} duplicate."
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_file_size(file: Any) -> None:
    """Reject files that exceed MAX_FILE_SIZE_MB."""
    if isinstance(file, (str, Path)):
        size_mb = Path(file).stat().st_size / (1024 ** 2)
    elif hasattr(file, "read"):
        pos = file.tell()
        file.seek(0, 2)                   # seek to end
        size_mb = file.tell() / (1024 ** 2)
        file.seek(pos)                    # rewind
    else:
        return                            # can't determine size — skip check

    if size_mb > MAX_FILE_SIZE_MB:
        raise ValueError(
            f"File is {size_mb:.1f} MB — exceeds the {MAX_FILE_SIZE_MB} MB limit."
        )


def _load_dataframe(file: Any) -> pd.DataFrame:
    """Read CSV from a path, file-like object, or raw bytes."""
    if isinstance(file, bytes):
        file = io.BytesIO(file)

    df = pd.read_csv(
        file,
        dtype=str,              # read everything as string — no silent type coercion
        skip_blank_lines=True,
        on_bad_lines="warn",    # log malformed rows instead of crashing
    )
    df.columns = df.columns.str.strip().str.lower()
    return df


def _validate_columns(df: pd.DataFrame) -> list[str]:
    """Return a list of error strings for missing required columns."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required column(s): {', '.join(sorted(missing))}. "
            f"Found: {', '.join(df.columns.tolist())}."
        )
    unknown = set(df.columns) - ALL_COLUMNS
    if unknown:
        logger.warning("Ignoring unrecognised column(s): %s", ", ".join(sorted(unknown)))
    return []


def _clean_str(value: Any) -> str | None:
    """Strip whitespace and return None for empty / NaN values."""
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def _is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email))


def _parse_row(row: pd.Series, row_num: int) -> tuple[ParsedContact | None, str | None]:
    """
    Validate and convert a single DataFrame row.

    Returns (ParsedContact, None) on success or (None, error_message) on failure.
    """
    name  = _clean_str(row.get("name"))
    email = _clean_str(row.get("email"))

    if not name:
        return None, f"Row {row_num}: missing name."
    if not email:
        return None, f"Row {row_num}: missing email."
    if not _is_valid_email(email):
        return None, f"Row {row_num}: invalid email {email!r}."

    return ParsedContact(
        name     = name,
        email    = email.lower(),
        company  = _clean_str(row.get("company")),
        role     = _clean_str(row.get("role")),
        linkedin = _clean_str(row.get("linkedin")),
        twitter  = _clean_str(row.get("twitter")),
    ), None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_contacts(
    file: str | Path | io.IOBase | bytes,
    *,
    skip_duplicates: bool = True,
) -> ParseResult:
    """
    Parse a CSV file into validated contact records.

    Args:
        file:            File path, file-like object, or raw bytes.
        skip_duplicates: If True, only the first occurrence of each email is kept.

    Returns:
        ParseResult with contacts and a full audit trail of skipped / duplicate rows.

    Raises:
        ValueError: On missing required columns or file size / row count violations.
    """
    _check_file_size(file)

    df = _load_dataframe(file)
    _validate_columns(df)

    total_rows = len(df)
    if total_rows == 0:
        raise ValueError("CSV file is empty.")
    if total_rows > MAX_ROWS:
        raise ValueError(
            f"CSV has {total_rows:,} rows — exceeds the {MAX_ROWS:,} row limit. "
            "Split the file into smaller batches."
        )

    logger.info("Parsing CSV | rows=%d columns=%s", total_rows, df.columns.tolist())

    contacts:  list[ParsedContact] = []
    errors:    list[str]           = []
    seen_emails: set[str]          = set()
    duplicate_rows = 0

    for i, (_, row) in enumerate(df.iterrows(), start=2):   # start=2 → Excel-style row numbers
        contact, error = _parse_row(row, row_num=i)

        if error:
            errors.append(error)
            logger.debug(error)
            continue

        if skip_duplicates and contact.email in seen_emails:
            duplicate_rows += 1
            logger.debug("Row %d: duplicate email %r — skipped.", i, contact.email)
            continue

        seen_emails.add(contact.email)
        contacts.append(contact)

    skipped_rows = len(errors)
    result = ParseResult(
        contacts=contacts,
        total_rows=total_rows,
        skipped_rows=skipped_rows,
        duplicate_rows=duplicate_rows,
        errors=errors,
    )

    logger.info(result.summary)
    return result