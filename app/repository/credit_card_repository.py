from uuid import UUID
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from ..models.credit_card import CreditCard


class CreditCardRepository:
    """Repository for CreditCard database operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_by_id(self, card_id: UUID) -> CreditCard | None:
        """Get credit card by ID."""
        result = await self.db.execute(select(CreditCard).filter(CreditCard.id == card_id))
        return result.scalar_one_or_none()
    
    async def get_by_id_and_user(self, card_id: UUID, user_id: UUID) -> CreditCard | None:
        """Get credit card by ID for a specific user."""
        result = await self.db.execute(
            select(CreditCard).filter(
                CreditCard.id == card_id,
                CreditCard.user_id == user_id
            )
        )
        return result.scalar_one_or_none()
    
    async def get_by_user(self, user_id: UUID) -> List[CreditCard]:
        """Get all credit cards for a user."""
        result = await self.db.execute(
            select(CreditCard)
            .filter(CreditCard.user_id == user_id)
            .order_by(CreditCard.is_default.desc(), CreditCard.created_at.desc())
        )
        return list(result.scalars().all())
    
    async def get_default_card(self, user_id: UUID) -> CreditCard | None:
        """Get user's default credit card."""
        result = await self.db.execute(
            select(CreditCard).filter(
                CreditCard.user_id == user_id,
                CreditCard.is_default == True
            )
        )
        return result.scalar_one_or_none()
    
    async def unset_default_cards(self, user_id: UUID) -> None:
        """Unset all default cards for a user."""
        await self.db.execute(
            update(CreditCard)
            .where(CreditCard.user_id == user_id)
            .values(is_default=False)
        )
        await self.db.commit()
    
    async def create(self, card: CreditCard) -> CreditCard:
        """Create a new credit card."""
        self.db.add(card)
        await self.db.commit()
        await self.db.refresh(card)
        return card
    
    async def update(self, card: CreditCard) -> CreditCard:
        """Update an existing credit card."""
        await self.db.commit()
        await self.db.refresh(card)
        return card
    
    async def delete(self, card: CreditCard) -> None:
        """Delete a credit card."""
        await self.db.delete(card)
        await self.db.commit()
    
    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.db.rollback()
