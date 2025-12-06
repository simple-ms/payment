import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import String, Boolean, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from ..database import Base


class CreditCard(Base):
    """Credit card model for storing encrypted payment methods."""
    
    __tablename__ = "credit_cards"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Card information (PCI compliant - only store last 4 digits and token)
    card_last_four: Mapped[str] = mapped_column(String(4), nullable=False)
    card_brand: Mapped[str] = mapped_column(String(20), nullable=False)  # visa, mastercard, amex, etc.
    card_token: Mapped[str] = mapped_column(String(500), nullable=False)  # Encrypted token from payment gateway
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=lambda: datetime.now(timezone.utc)
    )
    
    # Indexes
    __table_args__ = (
        Index('idx_credit_card_user', 'user_id'),
        Index('idx_credit_card_default', 'user_id', 'is_default'),
    )
    
    def __repr__(self) -> str:
        return f"<CreditCard(id={self.id}, user_id={self.user_id}, last_four={self.card_last_four})>"
