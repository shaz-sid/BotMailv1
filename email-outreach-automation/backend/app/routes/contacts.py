import logging
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from pydantic import BaseModel, EmailStr, field_validator
from sqlalchemy.orm import Session

from database import get_db
from models import Contact
from services.csv_service import parse_contacts, ParseResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/contacts", tags=["Contacts"])

# Reuse the session dependency from database.py — no need to redefine it here
DB = Annotated[Session, Depends(get_db)]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ContactBase(BaseModel):
    name:     str
    email:    EmailStr
    company:  str | None = None
    role:     str | None = None
    linkedin: str | None = None
    twitter:  str | None = None


class ContactCreate(ContactBase):
    pass


class ContactResponse(ContactBase):
    id:        int
    is_active: bool

    model_config = {"from_attributes": True}


class ContactListResponse(BaseModel):
    total:    int
    page:     int
    per_page: int
    contacts: list[ContactResponse]


class UploadResponse(BaseModel):
    total_rows:     int
    imported:       int
    skipped:        int
    duplicates:     int
    errors:         list[str]
    summary:        str


# ---------------------------------------------------------------------------
# GET /contacts
# ---------------------------------------------------------------------------

@router.get("/", response_model=ContactListResponse)
def list_contacts(
    db:       DB,
    page:     int = Query(1,   ge=1,   description="Page number"),
    per_page: int = Query(20,  ge=1,   le=100, description="Results per page"),
    search:   str = Query("",          description="Filter by name, email, or company"),
    active_only: bool = Query(True,    description="Exclude opted-out contacts"),
):
    """
    Return a paginated, optionally filtered list of contacts.
    """
    query = db.query(Contact)

    if active_only:
        query = query.filter(Contact.is_active == True)

    if search:
        like = f"%{search.strip()}%"
        query = query.filter(
            Contact.name.ilike(like)
            | Contact.email.ilike(like)
            | Contact.company.ilike(like)
        )

    total   = query.count()
    offset  = (page - 1) * per_page
    results = query.order_by(Contact.created_at.desc()).offset(offset).limit(per_page).all()

    return ContactListResponse(
        total    = total,
        page     = page,
        per_page = per_page,
        contacts = results,
    )


# ---------------------------------------------------------------------------
# GET /contacts/{contact_id}
# ---------------------------------------------------------------------------

@router.get("/{contact_id}", response_model=ContactResponse)
def get_contact(contact_id: int, db: DB):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contact not found.")
    return contact


# ---------------------------------------------------------------------------
# POST /contacts
# ---------------------------------------------------------------------------

@router.post("/", response_model=ContactResponse, status_code=status.HTTP_201_CREATED)
def create_contact(payload: ContactCreate, db: DB):
    """Create a single contact manually."""
    existing = db.query(Contact).filter(Contact.email == payload.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"A contact with email {payload.email!r} already exists.",
        )

    contact = Contact(**payload.model_dump())
    db.add(contact)
    db.commit()
    db.refresh(contact)

    logger.info("Contact created | id=%s email=%s", contact.id, contact.email)
    return contact


# ---------------------------------------------------------------------------
# POST /contacts/upload
# ---------------------------------------------------------------------------

@router.post("/upload", response_model=UploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_contacts(
    db:   DB,
    file: UploadFile = File(...),
):
    """
    Bulk-import contacts from a CSV file.

    - Validates every row before touching the database.
    - Skips rows with invalid / duplicate emails.
    - Upserts by email: existing contacts are updated, new ones are inserted.
    """
    # --- file type guard ---
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Only .csv files are accepted.",
        )

    raw = await file.read()
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty.",
        )

    # --- parse & validate via csv_service ---
    try:
        result: ParseResult = parse_contacts(raw)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))

    if not result.contacts:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"No valid contacts found. Issues: {result.errors[:5]}",
        )

    # --- upsert into DB ---
    imported = 0
    for parsed in result.contacts:
        existing = db.query(Contact).filter(Contact.email == parsed.email).first()

        if existing:
            # update fields that may have changed
            existing.name     = parsed.name
            existing.company  = parsed.company
            existing.role     = parsed.role
            existing.linkedin = parsed.linkedin
            existing.twitter  = parsed.twitter
        else:
            db.add(Contact(
                name     = parsed.name,
                email    = parsed.email,
                company  = parsed.company,
                role     = parsed.role,
                linkedin = parsed.linkedin,
                twitter  = parsed.twitter,
            ))
            imported += 1

    db.commit()

    logger.info(
        "CSV upload complete | imported=%d skipped=%d duplicates=%d",
        imported, result.skipped_rows, result.duplicate_rows,
    )

    return UploadResponse(
        total_rows  = result.total_rows,
        imported    = imported,
        skipped     = result.skipped_rows,
        duplicates  = result.duplicate_rows,
        errors      = result.errors,
        summary     = result.summary,
    )


# ---------------------------------------------------------------------------
# PATCH /contacts/{contact_id}
# ---------------------------------------------------------------------------

class ContactUpdate(BaseModel):
    name:     str | None = None
    company:  str | None = None
    role:     str | None = None
    linkedin: str | None = None
    twitter:  str | None = None
    is_active: bool | None = None

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, v: str | None) -> str | None:
        if v is not None and not v.strip():
            raise ValueError("Name must not be blank.")
        return v


@router.patch("/{contact_id}", response_model=ContactResponse)
def update_contact(contact_id: int, payload: ContactUpdate, db: DB):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contact not found.")

    updates = payload.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(contact, field, value)

    db.commit()
    db.refresh(contact)

    logger.info("Contact updated | id=%s fields=%s", contact.id, list(updates.keys()))
    return contact


# ---------------------------------------------------------------------------
# DELETE /contacts/{contact_id}
# ---------------------------------------------------------------------------

@router.delete("/{contact_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_contact(contact_id: int, db: DB):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contact not found.")

    db.delete(contact)
    db.commit()

    logger.info("Contact deleted | id=%s email=%s", contact.id, contact.email)