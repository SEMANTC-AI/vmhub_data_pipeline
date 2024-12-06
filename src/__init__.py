# src/__init__.py

"""VMHUB DATA PIPELINE PACKAGE"""
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()