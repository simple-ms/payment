import json
import logging
import asyncio
from typing import Callable, Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import select
from .settings import settings

from .database import AsyncSessionLocal
from .models import Payment, PaymentStatus

logger = logging.getLogger("payment-service")


class KafkaConsumerClient:
    """Kafka consumer client for consuming order events."""
    
    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id
        self.consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka consumer connection."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=self.group_id,

                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            logger.info(f"Kafka consumer connected to topic '{self.topic}' with group '{self.group_id}'")
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {str(e)}")
            self.consumer = None
    
    async def consume_async(self, handler: Callable[[Dict[str, Any]], Any]):
        """
        Async consumption of messages using the existing event loop.
        
        This is the preferred method for async applications as it doesn't
        create new event loops for each message.
        
        Args:
            handler: Async function to process each message
        """
        if not self.consumer:
            logger.error("Kafka consumer not initialized")
            return
        
        self._running = True
        logger.info(f"Starting async consumption from topic '{self.topic}'")
        
        try:
            while self._running:
                # Poll for messages with a timeout
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            logger.info(
                                f"Received message from partition {message.partition}, "
                                f"offset {message.offset}"
                            )
                            # Use await directly since we're in an async context
                            await handler(message.value)
                        except Exception as e:
                            logger.error(f"Error processing message: {str(e)}")
                
                # Yield control to allow other tasks to run
                await asyncio.sleep(0)
                
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
        finally:
            self.close()
    
    def consume(self, handler: Callable[[Dict[str, Any]], None]):
        """
        Synchronous consumption wrapper that properly handles async handlers.
        
        Creates a single event loop and runs the async consumer in it.
        This should be called from a synchronous context (like run_consumer.py).
        
        Args:
            handler: Async function to process each message
        """
        if not self.consumer:
            logger.error("Kafka consumer not initialized")
            return
        
        self._running = True
        logger.info(f"Starting to consume messages from topic '{self.topic}'")
        
        # Create a new event loop for this thread
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        try:
            self._loop.run_until_complete(self._consume_loop(handler))
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
        finally:
            self._running = False
            self.close()
            self._loop.close()
    
    async def _consume_loop(self, handler: Callable[[Dict[str, Any]], Any]):
        """Internal async consumption loop."""
        while self._running:
            # Poll for messages
            message_batch = self.consumer.poll(timeout_ms=1000)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    try:
                        logger.info(
                            f"Received message from partition {message.partition}, "
                            f"offset {message.offset}"
                        )
                        # Await the async handler
                        await handler(message.value)
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
            
            # Brief yield for responsiveness
            await asyncio.sleep(0.01)
    
    def stop(self):
        """Signal the consumer to stop."""
        self._running = False
        logger.info("Consumer stop requested")
    
    def close(self):
        """Close Kafka consumer connection."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {str(e)}")
            finally:
                self.consumer = None


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
