"""
StreamLog - A Kafka-like distributed commit log storage engine.

This package provides a lightweight implementation of an append-only
distributed log with segment rolling and topic management.
"""

__version__ = "2.0.0"
__author__ = "StreamLog Contributors"
__license__ = "MIT"

from .storage.segment import LogSegment
from .storage.partition import Partition

__all__ = ["LogSegment", "Partition"]
