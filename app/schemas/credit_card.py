import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator
import re


class CreditCardCreate(BaseModel):
    """Schema for creating a new credit card."""
    card_number: str = Field(..., min_length=13, max_length=19, description="Full card number (will be tokenized)")
    card_brand: str = Field(..., max_length=20, description="Card brand (visa, mastercard, amex, etc.)")
    expiry_month: int = Field(..., ge=1, le=12, description="Expiry month (1-12)")
    expiry_year: int = Field(..., ge=2024, description="Expiry year")
    cvv: str = Field(..., min_length=3, max_length=4, description="CVV code")
    is_default: bool = Field(default=False, description="Set as default payment method")
    
    @field_validator('card_number')
    @classmethod
    def validate_card_number(cls, v: str) -> str:
        """Validate card number format."""
        # Remove spaces and dashes
        card_num = re.sub(r'[\s-]', '', v)
        
        # Check if only digits
        if not card_num.isdigit():
            raise ValueError('Card number must contain only digits')
        
        # Check length
        if len(card_num) < 13 or len(card_num) > 19:
            raise ValueError('Card number must be between 13 and 19 digits')
        
        return card_num
    
    @field_validator('cvv')
    @classmethod
    def validate_cvv(cls, v: str) -> str:
        """Validate CVV format."""
        if not v.isdigit():
            raise ValueError('CVV must contain only digits')
        if len(v) not in [3, 4]:
            raise ValueError('CVV must be 3 or 4 digits')
        return v


class CreditCardResponse(BaseModel):
    """Schema for credit card response (never expose full card number)."""
    id: uuid.UUID
    user_id: uuid.UUID
    card_last_four: str
    card_brand: str
    is_default: bool
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class CreditCardUpdate(BaseModel):
    """Schema for updating credit card (only allow changing default status)."""
    is_default: bool = Field(..., description="Set as default payment method")
