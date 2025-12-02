import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Float, DateTime, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column
from .database import Base
import enum


class PaymentStatus(str, enum.Enum):
    """Payment status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class Payment(Base):
    """Payment model representing payment transactions."""
    
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(index=True)  # Link to order
    user_id: Mapped[uuid.UUID] = mapped_column(index=True)
    amount: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(
        SQLEnum(PaymentStatus),
        default=PaymentStatus.PENDING
    )
    payment_method: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, default="card")
    transaction_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)  # External payment gateway ID
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, onupdate=datetime.utcnow)
    
    def __repr__(self) -> str:
        return f"<Payment(id={self.id}, order_id={self.order_id}, status={self.status}, amount={self.amount})>"
