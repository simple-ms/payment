from .health import router as health_router
from .payment import router as payment_router
from .credit_card import router as credit_card_router

__all__ = ["health_router", "payment_router", "credit_card_router"]

