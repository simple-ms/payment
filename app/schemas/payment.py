import uuid
from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, field_validator


class PaymentCreate(BaseModel):
    """Schema for creating/processing a payment."""
    order_id: uuid.UUID = Field(..., description="Order ID to pay for")
    amount: float = Field(..., gt=0, le=100000, description="Payment amount (max $100,000)")
    payment_method: Literal["card", "paypal", "bank_transfer"] = Field(
        default="card",
        description="Payment method"
    )
    
    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: float) -> float:
        """Validate payment amount."""
        if v <= 0:
            raise ValueError('Payment amount must be greater than 0')
        if v > 100000:
            raise ValueError('Payment amount cannot exceed $100,000')
        # Round to 2 decimal places
        return round(v, 2)


class PaymentResponse(BaseModel):
    """Schema for payment response."""
    id: uuid.UUID
    order_id: uuid.UUID
    user_id: uuid.UUID
    amount: float
    status: str
    payment_method: Optional[str]
    transaction_id: Optional[str]
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

