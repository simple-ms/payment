import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger("payment-service")


class KafkaProducerClient:
    """Kafka producer client for publishing payment events."""
    
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka producer connection."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1  # Ensure ordering
            )
            logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {str(e)}")
            self.producer = None
    
    def send_event(self, topic: str, event_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Send an event to Kafka topic.
        
        Args:
            topic: Kafka topic name
            event_data: Event data dictionary
            key: Optional message key for partitioning
            
        Returns:
            True if successful, False otherwise
        """
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        try:
            future = self.producer.send(topic, value=event_data, key=key)
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Event sent to topic '{topic}': "
                f"partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to topic '{topic}': {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending event: {str(e)}")
            return False
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


# Global producer instance
kafka_producer = KafkaProducerClient()


def publish_payment_completed(payment_data: Dict[str, Any]) -> bool:
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
    return kafka_producer.send_event(
        topic="payment-events",
        event_data=event,
        key=str(payment_data["payment_id"])
    )


def publish_payment_failed(payment_data: Dict[str, Any], reason: str) -> bool:
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
    return kafka_producer.send_event(
        topic="payment-events",
        event_data=event,
        key=str(payment_data["payment_id"])
    )
