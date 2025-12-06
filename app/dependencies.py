from uuid import UUID
from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from .database import get_db
from .repository import PaymentRepository
from .repository.credit_card_repository import CreditCardRepository
from .services import PaymentService
from .services.credit_card_service import CreditCardService


async def get_payment_repository(db: AsyncSession = Depends(get_db)) -> PaymentRepository:
    """Dependency to get PaymentRepository instance."""
    return PaymentRepository(db)


async def get_payment_service(
    payment_repository: PaymentRepository = Depends(get_payment_repository)
) -> PaymentService:
    """Dependency to get PaymentService instance."""
    return PaymentService(payment_repository)


async def get_current_user_id(x_user_id: str = Header(..., alias="X-User-Id")) -> UUID:
    """
    Extract user ID from X-User-Id header set by Nginx after token validation.
    """
    try:
        return UUID(x_user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID in header"
        )


async def get_credit_card_repository(db: AsyncSession = Depends(get_db)) -> CreditCardRepository:
    """Dependency to get CreditCardRepository instance."""
    return CreditCardRepository(db)


async def get_credit_card_service(
    card_repository: CreditCardRepository = Depends(get_credit_card_repository)
) -> CreditCardService:
    """Dependency to get CreditCardService instance."""
    return CreditCardService(card_repository)
