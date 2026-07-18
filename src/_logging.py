"""
Shared logging configuration for every entry point (API, processor, scripts).

Importing loguru's `logger` alone inherits whatever sink happens to be
configured; calling `configure()` once at startup makes the format consistent
across services, which matters when their logs are read side by side.
"""

from __future__ import annotations

import sys

from loguru import logger

FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan> | "
    "<level>{message}</level> | {extra}"
)


def configure(service: str, level: str = "INFO") -> None:
    """Install a single stderr sink and tag every record with the service name."""
    logger.remove()
    logger.add(sys.stderr, level=level, format=FORMAT, colorize=True)
    logger.configure(extra={"service": service})


def get_logger(service: str, level: str = "INFO"):
    """Configure logging for `service` and return the shared logger."""
    configure(service, level)
    return logger
