#!/usr/bin/env python3
"""
Kafka consumer runner for Payment Service.
This script starts the async Kafka consumer to listen for order events.
"""

import sys
import signal
import logging
import asyncio
from .kafka.consumer import start_order_event_consumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal, stopping consumer...")
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Payment Service Async Kafka Consumer...")
    logger.info("Listening for order events...")
    
    try:
        asyncio.run(start_order_event_consumer())
    except Exception as e:
        logger.error(f"Consumer error: {str(e)}")
        sys.exit(1)
