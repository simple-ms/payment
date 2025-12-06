from uuid import UUID
from typing import List
from fastapi import HTTPException, status
from sqlalchemy.exc import SQLAlchemyError

from ..models.credit_card import CreditCard
from ..schemas.credit_card import CreditCardCreate, CreditCardUpdate
from ..repository.credit_card_repository import CreditCardRepository
from ..logger import logger


class CreditCardService:
    """Service for credit card business logic."""
    
    def __init__(self, card_repository: CreditCardRepository):
        self.card_repository = card_repository
    
    async def create_credit_card(self, user_id: UUID, card_data: CreditCardCreate) -> CreditCard:
        """
        Create a new credit card.
        
        This calls the payment gateway to tokenize the card securely.
        Only the token and last 4 digits are stored.
        """
        logger.info(f"Creating credit card for user {user_id}")
        
        try:
            # Import payment gateway
            from ..payment_gateway import payment_gateway
            
            # Tokenize card via payment gateway
            token_response = await payment_gateway.tokenize_card(
                card_number=card_data.card_number,
                exp_month=card_data.expiry_month,
                exp_year=card_data.expiry_year,
                cvc=card_data.cvv
            )
            
            # Extract token and card info
            card_token = token_response.get('id')
            card_info = token_response.get('card', {})
            last_four = card_info.get('last4', card_data.card_number[-4:])
            brand = card_info.get('brand', card_data.card_brand)
            
            logger.info(f"Card tokenized successfully: {brand} ending in {last_four}")
            
            # If this is set as default, unset other defaults
            if card_data.is_default:
                await self.card_repository.unset_default_cards(user_id)
            
            new_card = CreditCard(
                user_id=user_id,
                card_last_four=last_four,
                card_brand=brand,
                card_token=card_token,  # Encrypted token from payment gateway
                is_default=card_data.is_default
            )
            
            card = await self.card_repository.create(new_card)
            logger.info(f"Credit card created: {card.id}")
            return card
            
        except Exception as e:
            await self.card_repository.rollback()
            logger.error(f"Error creating credit card: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to save credit card: {str(e)}"
            )
    
    async def get_user_credit_cards(self, user_id: UUID) -> List[CreditCard]:
        """Get all credit cards for a user."""
        logger.info(f"Fetching credit cards for user {user_id}")
        
        try:
            cards = await self.card_repository.get_by_user(user_id)
            logger.info(f"Found {len(cards)} cards for user {user_id}")
            return cards
        except SQLAlchemyError as e:
            logger.error(f"Database error fetching credit cards: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch credit cards"
            )
    
    async def update_credit_card(
        self, 
        card_id: UUID, 
        user_id: UUID, 
        card_update: CreditCardUpdate
    ) -> CreditCard:
        """Update a credit card (set as default)."""
        logger.info(f"Updating credit card {card_id}")
        
        try:
            card = await self.card_repository.get_by_id_and_user(card_id, user_id)
            
            if not card:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Credit card not found"
                )
            
            # If setting as default, unset other defaults
            if card_update.is_default and not card.is_default:
                await self.card_repository.unset_default_cards(user_id)
            
            card.is_default = card_update.is_default
            updated_card = await self.card_repository.update(card)
            
            logger.info(f"Credit card {card_id} updated")
            return updated_card
            
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.card_repository.rollback()
            logger.error(f"Database error updating credit card: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update credit card"
            )
    
    async def delete_credit_card(self, card_id: UUID, user_id: UUID) -> None:
        """Delete a credit card."""
        logger.info(f"Deleting credit card {card_id}")
        
        try:
            card = await self.card_repository.get_by_id_and_user(card_id, user_id)
            
            if not card:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Credit card not found"
                )
            
            await self.card_repository.delete(card)
            logger.info(f"Credit card {card_id} deleted")
            
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.card_repository.rollback()
            logger.error(f"Database error deleting credit card: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete credit card"
            )
