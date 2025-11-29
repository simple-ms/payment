from fastapi import FastAPI, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .database import get_db, init_db
from . import models

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

class PaymentCreate(BaseModel):
    amount: float

@app.post("/pay")
def process_payment(payment: PaymentCreate, db: Session = Depends(get_db)):
    
    new_payment = models.Payment(
        amount=payment.amount,
        status="completed"
    )
    db.add(new_payment)
    db.commit()
    db.refresh(new_payment)
    return {
        "message": "Payment processed successfully",
        "amount": new_payment.amount,
        "payment_id": new_payment.id,
        "status": new_payment.status
    }