"""
Kafka integration package for Payment Service.
Provides async Kafka producer and consumer functionality.
"""
from .producer import (
    kafka_producer,
    publish_payment_completed,
    publish_payment_failed
)
from .consumer import start_order_event_consumer

__all__ = [
    "kafka_producer",
    "publish_payment_completed",
    "publish_payment_failed",
    "start_order_event_consumer"
]
