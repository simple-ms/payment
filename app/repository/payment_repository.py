from uuid import UUID
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.payment import Payment


class PaymentRepository:
    """Repository for Payment database operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        """Get payment by ID."""
        result = await self.db.execute(select(Payment).filter(Payment.id == payment_id))
        return result.scalar_one_or_none()
    
    async def get_by_order_id(self, order_id: UUID) -> Payment | None:
        """Get payment by order ID."""
        result = await self.db.execute(select(Payment).filter(Payment.order_id == order_id))
        return result.scalar_one_or_none()
    
    async def get_by_id_and_user(self, payment_id: UUID, user_id: UUID) -> Payment | None:
        """Get payment by ID for a specific user."""
        result = await self.db.execute(
            select(Payment).filter(
                Payment.id == payment_id,
                Payment.user_id == user_id
            )
        )
        return result.scalar_one_or_none()
    
    async def get_all_by_user(
        self, 
        user_id: UUID, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[Payment]:
        """Get all payments for a user with pagination."""
        result = await self.db.execute(
            select(Payment)
            .filter(Payment.user_id == user_id)
            .order_by(Payment.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())
    
    async def create(self, payment: Payment) -> Payment:
        """Create a new payment."""
        self.db.add(payment)
        await self.db.commit()
        await self.db.refresh(payment)
        return payment
    
    async def update(self, payment: Payment) -> Payment:
        """Update an existing payment."""
        await self.db.commit()
        await self.db.refresh(payment)
        return payment
    
    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.db.rollback()

