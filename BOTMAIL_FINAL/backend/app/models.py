from sqlalchemy import (
    Column, Integer, String, Text, Boolean,
    ForeignKey, DateTime, Enum, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship, validates
from sqlalchemy.sql import func
import enum
from database import Base


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class CampaignStatus(str, enum.Enum):
    draft      = "draft"
    active     = "active"
    paused     = "paused"
    completed  = "completed"
    archived   = "archived"


class EmailStatus(str, enum.Enum):
    pending   = "pending"
    queued    = "queued"
    sent      = "sent"
    failed    = "failed"
    bounced   = "bounced"
    opted_out = "opted_out"


class InteractionType(str, enum.Enum):
    opened   = "opened"
    clicked  = "clicked"
    replied  = "replied"
    bounced  = "bounced"
    opted_out = "opted_out"


# ---------------------------------------------------------------------------
# Contact
# ---------------------------------------------------------------------------

class Contact(Base):
    __tablename__ = "contacts"

    id         = Column(Integer, primary_key=True, index=True)
    name       = Column(String(255), nullable=False)
    email      = Column(String(320), unique=True, index=True, nullable=False)
    company    = Column(String(255))
    role       = Column(String(255))
    linkedin   = Column(String(500))
    twitter    = Column(String(255))
    is_active  = Column(Boolean, default=True, nullable=False)   # soft-delete / opt-out flag
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # relationships
    emails = relationship("Email", back_populates="contact", cascade="all, delete-orphan")

    @validates("email")
    def normalize_email(self, _key, value: str) -> str:
        return value.strip().lower()

    @validates("linkedin", "twitter")
    def strip_whitespace(self, _key, value: str | None) -> str | None:
        return value.strip() if value else value

    def __repr__(self) -> str:
        return f"<Contact id={self.id} email={self.email!r}>"


# ---------------------------------------------------------------------------
# Campaign
# ---------------------------------------------------------------------------

class Campaign(Base):
    __tablename__ = "campaigns"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String(255), nullable=False)
    description = Column(Text)
    status      = Column(
                    Enum(CampaignStatus, name="campaign_status"),
                    default=CampaignStatus.draft,
                    nullable=False,
                    index=True,
                  )
    created_at  = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # relationships
    emails = relationship("Email", back_populates="campaign", cascade="all, delete-orphan")

    @property
    def total_sent(self) -> int:
        return sum(1 for e in self.emails if e.status == EmailStatus.sent)

    @property
    def open_rate(self) -> float:
        sent = self.total_sent
        return (sum(1 for e in self.emails if e.opened) / sent) if sent else 0.0

    @property
    def reply_rate(self) -> float:
        sent = self.total_sent
        return (sum(1 for e in self.emails if e.replied) / sent) if sent else 0.0

    def __repr__(self) -> str:
        return f"<Campaign id={self.id} name={self.name!r} status={self.status}>"


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

class Email(Base):
    __tablename__ = "emails"

    __table_args__ = (
        UniqueConstraint("contact_id", "campaign_id", name="uq_email_contact_campaign"),
        Index("ix_emails_status_sent_at", "status", "sent_at"),
    )

    id          = Column(Integer, primary_key=True, index=True)
    contact_id  = Column(Integer, ForeignKey("contacts.id",  ondelete="CASCADE"), nullable=False, index=True)
    campaign_id = Column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False, index=True)
    subject     = Column(String(998))                          # RFC 2822 max subject length
    body        = Column(Text)
    status      = Column(
                    Enum(EmailStatus, name="email_status"),
                    default=EmailStatus.pending,
                    nullable=False,
                    index=True,
                  )
    sent_at     = Column(DateTime(timezone=True))
    opened      = Column(Boolean, default=False, nullable=False)
    replied     = Column(Boolean, default=False, nullable=False)
    clicked     = Column(Boolean, default=False, nullable=False)
    error_msg   = Column(Text)                                 # store failure reason
    created_at  = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # relationships
    contact      = relationship("Contact",  back_populates="emails")
    campaign     = relationship("Campaign", back_populates="emails")
    interactions = relationship("Interaction", back_populates="email", cascade="all, delete-orphan")

    def mark_sent(self) -> None:
        from datetime import datetime, timezone
        self.status  = EmailStatus.sent
        self.sent_at = datetime.now(timezone.utc)

    def mark_failed(self, reason: str) -> None:
        self.status    = EmailStatus.failed
        self.error_msg = reason

    def __repr__(self) -> str:
        return f"<Email id={self.id} status={self.status} contact_id={self.contact_id}>"


# ---------------------------------------------------------------------------
# Interaction
# ---------------------------------------------------------------------------

class Interaction(Base):
    __tablename__ = "interactions"

    __table_args__ = (
        Index("ix_interactions_email_event", "email_id", "event_type"),
    )

    id         = Column(Integer, primary_key=True, index=True)
    email_id   = Column(Integer, ForeignKey("emails.id", ondelete="CASCADE"), nullable=False, index=True)
    event_type = Column(
                   Enum(InteractionType, name="interaction_type"),
                   nullable=False,
                   index=True,
                 )
    ip_address = Column(String(45))     # IPv4 or IPv6
    user_agent = Column(String(500))
    metadata_  = Column("metadata", Text)   # JSON blob for extra event data
    timestamp  = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)

    # relationship
    email = relationship("Email", back_populates="interactions")

    def __repr__(self) -> str:
        return f"<Interaction id={self.id} email_id={self.email_id} event={self.event_type}>"