from uuid import UUID
from typing import List
from fastapi import APIRouter, Depends, status

from ..schemas.credit_card import CreditCardCreate, CreditCardResponse, CreditCardUpdate
from ..services import CreditCardService
from ..dependencies import get_credit_card_service, get_current_user_id

router = APIRouter(tags=["Credit Cards"])


@router.post(
    "/credit-cards",
    response_model=CreditCardResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_credit_card(
    card_data: CreditCardCreate,
    user_id: UUID = Depends(get_current_user_id),
    card_service: CreditCardService = Depends(get_credit_card_service)
):
    """
    Save a new credit card.
    
    The card number will be tokenized and only the last 4 digits will be stored.
    """
    return await card_service.create_credit_card(user_id, card_data)


@router.get(
    "/credit-cards",
    response_model=List[CreditCardResponse]
)
async def get_credit_cards(
    user_id: UUID = Depends(get_current_user_id),
    card_service: CreditCardService = Depends(get_credit_card_service)
):
    """
    Get all saved credit cards for the authenticated user.
    """
    return await card_service.get_user_credit_cards(user_id)


@router.patch(
    "/credit-cards/{card_id}",
    response_model=CreditCardResponse
)
async def update_credit_card(
    card_id: UUID,
    card_update: CreditCardUpdate,
    user_id: UUID = Depends(get_current_user_id),
    card_service: CreditCardService = Depends(get_credit_card_service)
):
    """
    Update a credit card (set as default).
    """
    return await card_service.update_credit_card(card_id, user_id, card_update)


@router.delete(
    "/credit-cards/{card_id}",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_credit_card(
    card_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    card_service: CreditCardService = Depends(get_credit_card_service)
):
    """
    Delete a saved credit card.
    """
    await card_service.delete_credit_card(card_id, user_id)
