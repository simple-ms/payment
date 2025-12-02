from fastapi import FastAPI, Depends
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from .database import get_db
from . import models
from .logger import logger
from .schemas import PaymentCreate

app = FastAPI(
    docs_url="/docs/payment",
    openapi_url="/openapi.json/payment",
    redoc_url="/redoc/payment"
)

security = HTTPBearer()

@app.post("/pay")
def process_payment(
    payment: PaymentCreate, 
    db: Session = Depends(get_db),
    token: str = Depends(security)
):
    logger.info(f"Processing payment: amount ${payment.amount}")
    
    new_payment = models.Payment(
        amount=payment.amount,
        status="completed"
    )
    db.add(new_payment)
    db.commit()
    db.refresh(new_payment)
    logger.info(f"Payment processed successfully: Payment ID {new_payment.id}, amount ${new_payment.amount}")
    return {
        "message": "Payment processed successfully",
        "amount": new_payment.amount,
        "payment_id": new_payment.id,
        "status": new_payment.status
    }