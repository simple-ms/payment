"""
Payment Service - Payment Processing Microservice

This is the main entry point for the Payment service.
All routes are defined in views/ and registered via routes.py
"""
from fastapi import FastAPI

from .routes import register_routes

app = FastAPI(
    title="Payment Service",
    description="Payment processing microservice with Kafka event publishing",
    version="1.0.0",
    docs_url="/docs/payment",
    openapi_url="/openapi.json/payment",
    redoc_url="/redoc/payment"
)

# NOTE: CORS is handled by nginx gateway - no CORS middleware here

# Register all routes
register_routes(app)
