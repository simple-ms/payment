from uuid import UUID
from typing import List
from fastapi import APIRouter, Depends, status

from ..schemas.payment import PaymentCreate, PaymentResponse
from ..services import PaymentService
from ..dependencies import get_payment_service, get_current_user_id

router = APIRouter(tags=["Payments"])


@router.post(
    "/payments",
    response_model=PaymentResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_payment(
    payment_data: PaymentCreate,
    user_id: UUID = Depends(get_current_user_id),
    payment_service: PaymentService = Depends(get_payment_service)
):
    """
    Process a payment for an order.
    
    This endpoint:
    1. Verifies the order exists and belongs to the user
    2. Validates the payment amount matches the order total
    3. Creates a payment record
    4. Processes the payment (simulated)
    5. Updates the order status to PAID on success
    6. Publishes payment_completed event to Kafka
    
    User ID is extracted from X-User-Id header (set by Nginx after token validation).
    """
    return await payment_service.process_payment(user_id, payment_data)


@router.get(
    "/payments",
    response_model=List[PaymentResponse]
)
async def get_payments(
    skip: int = 0,
    limit: int = 100,
    user_id: UUID = Depends(get_current_user_id),
    payment_service: PaymentService = Depends(get_payment_service)
):
    """
    Get all payments for the authenticated user with pagination.
    
    User ID is extracted from X-User-Id header (set by Nginx after token validation).
    """
    return await payment_service.get_user_payments(user_id, skip, limit)


@router.get(
    "/payments/{payment_id}",
    response_model=PaymentResponse
)
async def get_payment(
    payment_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    payment_service: PaymentService = Depends(get_payment_service)
):
    """Get a specific payment by ID."""
    return await payment_service.get_payment(payment_id, user_id)


@router.post(
    "/payments/{payment_id}/refund",
    response_model=PaymentResponse
)
async def refund_payment(
    payment_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    payment_service: PaymentService = Depends(get_payment_service)
):
    """
    Request a refund for a completed payment.
    
    Only completed payments can be refunded.
    """
    return await payment_service.refund_payment(payment_id, user_id)

