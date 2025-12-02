import uuid
from typing import Optional
from pydantic import BaseModel, Field


class PaymentCreate(BaseModel):
    """Schema for creating/processing a payment."""
    order_id: uuid.UUID = Field(..., description="Order ID to pay for")
    amount: float = Field(..., gt=0, description="Payment amount")
    payment_method: Optional[str] = Field("card", description="Payment method")


class PaymentResponse(BaseModel):
    """Schema for payment response."""
    id: uuid.UUID
    order_id: uuid.UUID
    user_id: uuid.UUID
    amount: float
    status: str
    payment_method: Optional[str]
    transaction_id: Optional[str]
    
    class Config:
        from_attributes = True