"""
Payment Gateway Integration Module

This module provides integration with payment gateways like Stripe.
For production use, replace the simulation with actual gateway API calls.
"""
import os
from typing import Dict, Optional
from uuid import UUID
import httpx
from ..logger import logger


class PaymentGateway:
    """Payment gateway integration (Stripe-compatible interface)."""
    
    def __init__(self):
        self.api_key = os.getenv('STRIPE_SECRET_KEY', 'sk_test_simulated')
        self.api_url = os.getenv('STRIPE_API_URL', 'https://api.stripe.com/v1')
        self.is_simulation = self.api_key.startswith('sk_test_simulated')
        
        if self.is_simulation:
            logger.warning("Payment gateway running in SIMULATION mode")
        else:
            logger.info("Payment gateway configured for production")
    
    async def create_payment_intent(
        self,
        amount: float,
        currency: str = "usd",
        customer_id: Optional[str] = None,
        payment_method_id: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """
        Create a payment intent.
        
        In production, this calls Stripe's API.
        In simulation, returns a mock response.
        """
        if self.is_simulation:
            return self._simulate_payment_intent(amount, currency, metadata)
        
        # Production implementation
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.api_url}/payment_intents",
                auth=(self.api_key, ""),
                data={
                    "amount": int(amount * 100),  # Convert to cents
                    "currency": currency,
                    "customer": customer_id,
                    "payment_method": payment_method_id,
                    "metadata": metadata or {},
                    "confirm": True,
                    "automatic_payment_methods": {"enabled": True}
                }
            )
            response.raise_for_status()
            return response.json()
    
    async def tokenize_card(
        self,
        card_number: str,
        exp_month: int,
        exp_year: int,
        cvc: str
    ) -> Dict:
        """
        Tokenize a credit card.
        
        In production, calls Stripe's tokenization API.
        In simulation, returns a mock token.
        """
        if self.is_simulation:
            return self._simulate_card_token(card_number)
        
        # Production implementation
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.api_url}/tokens",
                auth=(self.api_key, ""),
                data={
                    "card[number]": card_number,
                    "card[exp_month]": exp_month,
                    "card[exp_year]": exp_year,
                    "card[cvc]": cvc
                }
            )
            response.raise_for_status()
            return response.json()
    
    async def create_customer(
        self,
        email: str,
        user_id: UUID,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """Create a customer in the payment gateway."""
        if self.is_simulation:
            return {
                "id": f"cus_simulated_{user_id}",
                "email": email,
                "created": 1234567890
            }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.api_url}/customers",
                auth=(self.api_key, ""),
                data={
                    "email": email,
                    "metadata": {
                        "user_id": str(user_id),
                        **(metadata or {})
                    }
                }
            )
            response.raise_for_status()
            return response.json()
    
    async def charge_saved_card(
        self,
        amount: float,
        payment_method_id: str,
        customer_id: str,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """Charge a saved payment method."""
        return await self.create_payment_intent(
            amount=amount,
            customer_id=customer_id,
            payment_method_id=payment_method_id,
            metadata=metadata
        )
    
    # Simulation methods
    def _simulate_payment_intent(self, amount: float, currency: str, metadata: Optional[Dict]) -> Dict:
        """Simulate payment intent creation."""
        logger.info(f"SIMULATION: Creating payment intent for {amount} {currency}")
        return {
            "id": f"pi_simulated_{int(amount * 100)}",
            "amount": int(amount * 100),
            "currency": currency,
            "status": "succeeded",
            "metadata": metadata or {},
            "created": 1234567890
        }
    
    def _simulate_card_token(self, card_number: str) -> Dict:
        """Simulate card tokenization."""
        last_four = card_number[-4:]
        logger.info(f"SIMULATION: Tokenizing card ending in {last_four}")
        
        # Determine card brand from number
        if card_number.startswith('4'):
            brand = 'visa'
        elif card_number.startswith(('51', '52', '53', '54', '55')):
            brand = 'mastercard'
        elif card_number.startswith(('34', '37')):
            brand = 'amex'
        else:
            brand = 'unknown'
        
        return {
            "id": f"tok_simulated_{last_four}",
            "card": {
                "last4": last_four,
                "brand": brand,
                "exp_month": 12,
                "exp_year": 2025
            },
            "created": 1234567890
        }


# Global instance
payment_gateway = PaymentGateway()
