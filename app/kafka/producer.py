"""
Async Kafka producer for Payment Service.
Publishes payment events using aiokafka.
"""
import json
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from ..settings import settings
from ..logger import logger


class AsyncKafkaProducerClient:
    """Async Kafka producer client for publishing payment events."""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self._started = False
    
    async def start(self):
        """Initialize and start Kafka producer connection."""
        if self._started:
            return
        
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                enable_idempotence=True,
                max_in_flight_requests_per_connection=1
            )
            await self.producer.start()
            self._started = True
            logger.info(f"Async Kafka producer connected to {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to start async Kafka producer: {str(e)}")
            self.producer = None
            self._started = False
    
    async def stop(self):
        """Stop Kafka producer connection."""
        if self.producer and self._started:
            await self.producer.stop()
            self._started = False
            logger.info("Async Kafka producer stopped")
    
    async def send_event(self, topic: str, event_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Send an event to Kafka topic.
        
        Args:
            topic: Kafka topic name
            event_data: Event data dictionary
            key: Optional message key for partitioning
            
        Returns:
            True if successful, False otherwise
        """
        if not self.producer or not self._started:
            await self.start()
        
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        try:
            metadata = await self.producer.send_and_wait(topic, value=event_data, key=key)
            logger.info(
                f"Event sent to topic '{topic}': "
                f"partition={metadata.partition}, offset={metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to topic '{topic}': {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending event: {str(e)}")
            return False


# Global producer instance
kafka_producer = AsyncKafkaProducerClient()


async def publish_payment_completed(payment_data: Dict[str, Any]) -> bool:
    """
    Publish payment_completed event.
    
    Args:
        payment_data: Payment information
        
    Returns:
        True if published successfully
    """
    event = {
        "event_type": "payment_completed",
        "payment_id": str(payment_data["payment_id"]),
        "order_id": str(payment_data["order_id"]),
        "user_id": str(payment_data["user_id"]),
        "amount": payment_data["amount"],
        "status": payment_data["status"]
    }
    return await kafka_producer.send_event(
        topic="payment-events",
        event_data=event,
        key=str(payment_data["payment_id"])
    )


async def publish_payment_failed(payment_data: Dict[str, Any], reason: str) -> bool:
    """
    Publish payment_failed event.
    
    Args:
        payment_data: Payment information
        reason: Failure reason
        
    Returns:
        True if published successfully
    """
    event = {
        "event_type": "payment_failed",
        "payment_id": str(payment_data["payment_id"]),
        "order_id": str(payment_data["order_id"]),
        "user_id": str(payment_data["user_id"]),
        "amount": payment_data["amount"],
        "reason": reason
    }
    return await kafka_producer.send_event(
        topic="payment-events",
        event_data=event,
        key=str(payment_data["payment_id"])
    )
