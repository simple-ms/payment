import uuid
from typing import List
from fastapi import FastAPI, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

from .database import get_db
from .models import Payment, PaymentStatus
from .schemas import PaymentCreate, PaymentResponse
from .logger import logger
from .kafka_producer import publish_payment_completed, publish_payment_failed
from .dependencies import get_current_user_id

app = FastAPI(
    title="Payment Service",
    description="Payment processing microservice with Kafka integration",
    version="1.0.0",
    docs_url="/docs/payment",
    openapi_url="/openapi.json/payment",
    redoc_url="/redoc/payment"
)


# --- HEALTH CHECK ---

@app.get("/payment/health", tags=["Health"])
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "payment-service"}


# --- PAYMENT ENDPOINTS ---

@app.post(
    "/pay",
    response_model=PaymentResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Payments"]
)
async def process_payment(
    payment: PaymentCreate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Process a payment for an order.
    
    Steps:
    1. Validate user authentication
    2. Check if payment already exists for the order
    3. Create/update payment record
    4. Simulate payment processing
    5. Publish payment_completed or payment_failed event to Kafka
    """
    logger.info(f"Processing payment for order {payment.order_id} by user {user_id}, amount: ${payment.amount}")
    
    try:
        # Check if payment already exists for this order
        result = await db.execute(select(Payment).filter(Payment.order_id == payment.order_id))
        existing_payment = result.scalar_one_or_none()
        
        if existing_payment:
            # Update existing payment
            if existing_payment.status == PaymentStatus.COMPLETED:
                logger.warning(f"Payment already completed for order {payment.order_id}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Payment already completed for this order"
                )
            
            logger.info(f"Updating existing payment {existing_payment.id} for order {payment.order_id}")
            existing_payment.status = PaymentStatus.PROCESSING
            existing_payment.amount = payment.amount
            existing_payment.payment_method = payment.payment_method
            await db.commit()
            await db.refresh(existing_payment)
            current_payment = existing_payment
        else:
            # Create new payment
            logger.info(f"Creating new payment for order {payment.order_id}")
            new_payment = Payment(
                order_id=payment.order_id,
                user_id=uuid.UUID(user_id),
                amount=payment.amount,
                status=PaymentStatus.PROCESSING,
                payment_method=payment.payment_method
            )
            db.add(new_payment)
            await db.commit()
            await db.refresh(new_payment)
            current_payment = new_payment
        
        # Simulate payment processing
        # In a real scenario, this would integrate with a payment gateway (Stripe, PayPal, etc.)
        payment_successful = simulate_payment_processing(payment.amount)
        
        if payment_successful:
            # Mark payment as completed
            current_payment.status = PaymentStatus.COMPLETED
            current_payment.transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
            await db.commit()
            await db.refresh(current_payment)
            
            logger.info(
                f"Payment processed successfully: Payment ID {current_payment.id}, "
                f"Order ID {payment.order_id}, Amount ${payment.amount}"
            )
            
            # Publish payment_completed event to Kafka
            event_published = publish_payment_completed({
                "payment_id": current_payment.id,
                "order_id": current_payment.order_id,
                "user_id": current_payment.user_id,
                "amount": current_payment.amount,
                "status": current_payment.status.value
            })
            
            if event_published:
                logger.info(f"payment_completed event published for payment {current_payment.id}")
            else:
                logger.warning(f"Failed to publish payment_completed event for payment {current_payment.id}")
            
            return current_payment
        else:
            # Mark payment as failed
            current_payment.status = PaymentStatus.FAILED
            await db.commit()
            await db.refresh(current_payment)
            
            logger.warning(f"Payment processing failed for order {payment.order_id}")
            
            # Publish payment_failed event to Kafka
            event_published = publish_payment_failed(
                {
                    "payment_id": current_payment.id,
                    "order_id": current_payment.order_id,
                    "user_id": current_payment.user_id,
                    "amount": current_payment.amount
                },
                reason="Payment gateway declined the transaction"
            )
            
            if event_published:
                logger.info(f"payment_failed event published for payment {current_payment.id}")
            else:
                logger.warning(f"Failed to publish payment_failed event for payment {current_payment.id}")
            
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Payment processing failed"
            )
        
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Database error while processing payment: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Unexpected error while processing payment: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred"
        )


@app.get(
    "/payments",
    response_model=List[PaymentResponse],
    tags=["Payments"]
)
async def get_user_payments(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get all payments for the authenticated user.
    """
    logger.info(f"Fetching payments for user {user_id}")
    
    try:
        result = await db.execute(select(Payment).filter(Payment.user_id == user_id))
        payments = result.scalars().all()
        
        logger.info(f"Found {len(payments)} payments for user {user_id}")
        return payments
        
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching payments: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.get(
    "/payments/{payment_id}",
    response_model=PaymentResponse,
    tags=["Payments"]
)
async def get_payment(
    payment_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get a specific payment by ID.
    
    Only the owner of the payment can view it.
    """
    logger.info(f"Fetching payment {payment_id} for user {user_id}")
    
    try:
        result = await db.execute(
            select(Payment).filter(
                Payment.id == payment_id,
                Payment.user_id == user_id  # Ensure user owns this payment
            )
        )
        payment = result.scalar_one_or_none()
        
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


@app.get(
    "/payments/order/{order_id}",
    response_model=PaymentResponse,
    tags=["Payments"]
)
async def get_payment_by_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get payment for a specific order.
    
    Only the owner of the payment can view it.
    """
    logger.info(f"Fetching payment for order {order_id} by user {user_id}")
    
    try:
        result = await db.execute(
            select(Payment).filter(
                Payment.order_id == order_id,
                Payment.user_id == user_id  # Ensure user owns this payment
            )
        )
        payment = result.scalar_one_or_none()
        
        if not payment:
            logger.warning(f"Payment not found for order {order_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Payment not found for this order"
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


# --- HELPER FUNCTIONS ---

def simulate_payment_processing(amount: float) -> bool:
    """
    Simulate payment processing.
    
    In a real application, this would integrate with a payment gateway.
    For now, we'll simulate success for amounts less than 10000.
    
    Args:
        amount: Payment amount
        
    Returns:
        True if payment successful, False otherwise
    """
    # Simulate payment gateway processing
    # In reality, you would call Stripe, PayPal, etc. here
    
    # Simple simulation: fail if amount is too high
    if amount > 10000:
        logger.warning(f"Simulated payment failure: amount ${amount} exceeds limit")
        return False
    
    logger.info(f"Simulated payment success for amount ${amount}")
    return True