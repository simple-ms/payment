import uuid
import httpx
from uuid import UUID
from typing import List, Dict
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ..models.payment import Payment, PaymentStatus
from ..schemas.payment import PaymentCreate, PaymentResponse
from ..repository import PaymentRepository
from ..settings import settings
from ..logger import logger
from ..kafka_producer import publish_payment_completed, publish_payment_failed


class PaymentService:
    """Service for payment business logic."""
    
    def __init__(self, payment_repository: PaymentRepository):
        self.payment_repository = payment_repository
    
    async def process_payment(self, user_id: UUID, payment_data: PaymentCreate) -> Payment:
        """
        Process a payment for an order.
        
        Steps:
        1. Verify order exists and belongs to user
        2. Check for existing payment (idempotency)
        3. Create payment record
        4. Simulate payment processing
        5. Update order status
        6. Publish Kafka event
        """
        logger.info(f"Payment request from user {user_id} for order {payment_data.order_id}")
        
        # Step 1: Verify order exists and belongs to user
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                order_response = await client.get(
                    f"{settings.ORDER_API_URL}/orders/{payment_data.order_id}",
                    headers={"X-User-Id": str(user_id)}
                )
            except httpx.RequestError as e:
                logger.error(f"Order service unavailable: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Order service unavailable"
                )
            
            if order_response.status_code == 404:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Order not found"
                )
            elif order_response.status_code != 200:
                logger.error(f"Failed to fetch order: {order_response.status_code}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to verify order"
                )
            
            order = order_response.json()
            
            # Verify amount matches order total
            if abs(order["total_amount"] - payment_data.amount) > 0.01:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error_code": "AMOUNT_MISMATCH",
                        "message": "Payment amount does not match order total",
                        "details": {
                            "order_total": order["total_amount"],
                            "payment_amount": payment_data.amount
                        }
                    }
                )
        
        # Step 2: Check for existing payment (idempotency)
        existing_payment = await self.payment_repository.get_by_order_id(payment_data.order_id)
        if existing_payment:
            logger.info(f"Existing payment found for order {payment_data.order_id}")
            if existing_payment.status == PaymentStatus.COMPLETED:
                return existing_payment
            elif existing_payment.status == PaymentStatus.PENDING:
                logger.info(f"Retrying payment for order {payment_data.order_id}")
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Payment already exists with status: {existing_payment.status.value}"
                )
        
        try:
            # Step 3: Create or update payment record
            if existing_payment:
                payment = existing_payment
                payment.status = PaymentStatus.PROCESSING
            else:
                payment = Payment(
                    order_id=payment_data.order_id,
                    user_id=user_id,
                    amount=payment_data.amount,
                    payment_method=payment_data.payment_method,
                    status=PaymentStatus.PROCESSING
                )
                payment = await self.payment_repository.create(payment)
            
            logger.info(f"Payment record created: {payment.id}")
            
            # Step 4: Simulate payment processing (in real system, call payment gateway)
            payment_success = await self._simulate_payment_processing(payment)
            
            if payment_success:
                # Step 5a: Mark payment as completed
                payment.status = PaymentStatus.COMPLETED
                payment.transaction_id = f"TXN_{uuid.uuid4().hex[:16].upper()}"
                await self.payment_repository.update(payment)
                
                logger.info(f"Payment completed: {payment.id}, transaction: {payment.transaction_id}")
                
                # Step 6a: Update order status to PAID
                async with httpx.AsyncClient(timeout=10.0) as client:
                    try:
                        await client.patch(
                            f"{settings.ORDER_API_URL}/orders/{payment_data.order_id}/status",
                            json={"status": "paid"},
                            headers={"X-User-Id": str(user_id), "X-User-Role": "system"}
                        )
                        logger.info(f"Order {payment_data.order_id} status updated to PAID")
                    except httpx.RequestError as e:
                        logger.error(f"Failed to update order status: {str(e)}")
                
                # Step 7a: Publish payment_completed event
                event_published = publish_payment_completed({
                    "payment_id": payment.id,
                    "order_id": payment.order_id,
                    "user_id": payment.user_id,
                    "amount": payment.amount,
                    "status": payment.status.value
                })
                
                if event_published:
                    logger.info(f"payment_completed event published for payment {payment.id}")
                else:
                    logger.warning(f"Failed to publish payment_completed event for payment {payment.id}")
            else:
                # Step 5b: Mark payment as failed
                payment.status = PaymentStatus.FAILED
                await self.payment_repository.update(payment)
                
                logger.warning(f"Payment failed: {payment.id}")
                
                # Step 6b: Publish payment_failed event
                publish_payment_failed({
                    "payment_id": payment.id,
                    "order_id": payment.order_id,
                    "user_id": payment.user_id,
                    "amount": payment.amount
                }, reason="Payment processing failed")
                
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Payment processing failed"
                )
            
            return payment
            
        except IntegrityError:
            await self.payment_repository.rollback()
            logger.warning(f"Duplicate payment attempt for order {payment_data.order_id}")
            
            # Return existing payment if it was a race condition
            existing = await self.payment_repository.get_by_order_id(payment_data.order_id)
            if existing:
                return existing
            
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Payment already exists for this order"
            )
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.payment_repository.rollback()
            logger.error(f"Database error while processing payment: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def _simulate_payment_processing(self, payment: Payment) -> bool:
        """
        Simulate payment processing.
        
        In production, this would integrate with actual payment gateways
        like Stripe, PayPal, etc.
        """
        # Simple simulation: payments succeed most of the time
        import random
        return random.random() > 0.1  # 90% success rate
    
    async def get_payment(self, payment_id: UUID, user_id: UUID) -> Payment:
        """Get a specific payment by ID."""
        logger.info(f"Fetching payment {payment_id} for user {user_id}")
        
        try:
            payment = await self.payment_repository.get_by_id_and_user(payment_id, user_id)
            
            if not payment:
                logger.warning(f"Payment not found or unauthorized: {payment_id}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Payment not found"
                )
            
            return payment
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching payment: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def get_user_payments(
        self, 
        user_id: UUID, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[Payment]:
        """Get all payments for a user with pagination."""
        limit = min(limit, 100)  # Cap the limit
        
        logger.info(f"Fetching payments for user {user_id}: skip={skip}, limit={limit}")
        
        try:
            payments = await self.payment_repository.get_all_by_user(user_id, skip, limit)
            logger.info(f"Found {len(payments)} payments for user {user_id}")
            return payments
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching payments: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def refund_payment(self, payment_id: UUID, user_id: UUID) -> Payment:
        """Process a refund for a payment."""
        logger.info(f"Refund request for payment {payment_id} from user {user_id}")
        
        try:
            payment = await self.payment_repository.get_by_id_and_user(payment_id, user_id)
            
            if not payment:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Payment not found"
                )
            
            if payment.status != PaymentStatus.COMPLETED:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot refund payment with status: {payment.status.value}"
                )
            
            # Process refund (simulated)
            payment.status = PaymentStatus.REFUNDED
            await self.payment_repository.update(payment)
            
            logger.info(f"Payment refunded: {payment_id}")
            return payment
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.payment_repository.rollback()
            logger.error(f"Database error while refunding payment: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )

