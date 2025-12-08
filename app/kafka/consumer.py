"""
Async Kafka consumer for Payment Service.
Listens for order events using aiokafka.
"""
import json
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select

from ..settings import settings
from ..database import AsyncSessionLocal
from ..models import Payment, PaymentStatus
from ..logger import logger


async def handle_order_event(event: Dict[str, Any]):
    """
    Handle order events - create payment record when order is created.
    
    Args:
        event: Order event data
    """
    event_type = event.get("event_type")
    
    if event_type != "order_created":
        logger.debug(f"Ignoring event type: {event_type}")
        return
    
    order_id = event.get("order_id")
    user_id = event.get("user_id")
    total_amount = event.get("total_amount")
    
    if not all([order_id, user_id, total_amount]):
        logger.warning(f"Incomplete order event data: {event}")
        return
    
    logger.info(f"Processing order_created event for order {order_id}")
    
    async with AsyncSessionLocal() as db:
        try:
            # Check if payment already exists (idempotency)
            result = await db.execute(select(Payment).filter(Payment.order_id == order_id))
            existing_payment = result.scalar_one_or_none()
            
            if existing_payment:
                logger.info(f"Payment already exists for order {order_id}")
                return
            
            # Create payment record in PENDING status
            payment = Payment(
                order_id=order_id,
                user_id=user_id,
                amount=total_amount,
                status=PaymentStatus.PENDING
            )
            db.add(payment)
            await db.commit()
            
            logger.info(f"Payment record created for order {order_id}: payment_id={payment.id}")
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating payment record: {str(e)}")


async def start_order_event_consumer():
    """Start the async Kafka consumer for order events."""
    logger.info("Starting Payment Service Async Kafka Consumer...")
    
    consumer = AIOKafkaConsumer(
        "order-events",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="payment-service-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    await consumer.start()
    logger.info("Listening for order events...")
    
    try:
        async for message in consumer:
            try:
                logger.info(
                    f"Received message from partition {message.partition}, "
                    f"offset {message.offset}"
                )
                await handle_order_event(message.value)
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")


if __name__ == "__main__":
    import asyncio
    asyncio.run(start_order_event_consumer())
