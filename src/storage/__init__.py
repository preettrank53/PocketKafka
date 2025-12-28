"""Storage package - Core log storage components."""

from .segment import LogSegment
from .partition import Partition

__all__ = ["LogSegment", "Partition"]
