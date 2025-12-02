import json
import logging
import asyncio
from typing import Callable, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import select
from .config import KAFKA_BOOTSTRAP_SERVERS
from .database import AsyncSessionLocal
from .models import Payment, PaymentStatus

logger = logging.getLogger("payment-service")


class KafkaConsumerClient:
    """Kafka consumer client for consuming order events."""
    
    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka consumer connection."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            logger.info(f"Kafka consumer connected to topic '{self.topic}' with group '{self.group_id}'")
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {str(e)}")
            self.consumer = None
    
    def consume(self, handler: Callable[[Dict[str, Any]], None]):
        """
        Start consuming messages and process with handler.
        
        Args:
            handler: Async function to process each message
        """
        if not self.consumer:
            logger.error("Kafka consumer not initialized")
            return
        
        logger.info(f"Starting to consume messages from topic '{self.topic}'")
        
        try:
            for message in self.consumer:
                try:
                    logger.info(f"Received message from partition {message.partition}, offset {message.offset}")
                    # Run async handler in event loop
                    asyncio.run(handler(message.value))
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
        finally:
            self.close()
    
    def close(self):
        """Close Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


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


def start_order_event_consumer():
    """Start consuming order events."""
    consumer = KafkaConsumerClient(
        topic="order-events",
        group_id="payment-service-group"
    )
    consumer.consume(handle_order_event)
