"""
Partition - Manages multiple LogSegment files for a specific topic-partition.

A Partition is responsible for:
1. Managing multiple segments (active + closed)
2. Segment rolling when size limits are reached
3. Routing reads to the correct segment based on offset
4. Loading existing segments from disk on startup
"""

import os
from pathlib import Path
from typing import List, Optional
from .segment import LogSegment


class Partition:
    """
    Manages multiple LogSegment files for a specific topic-partition.
    
    Handles segment rotation, load balancing, and offset management across
    multiple segment files.
    """
    
    # Segment size limit - when exceeded, create a new segment
    # Using 1MB for production-like behavior, but can be set lower for testing
    SEGMENT_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB in bytes
    
    def __init__(self, topic: str, partition_id: int, data_dir: str = "data", 
                 segment_size_limit: Optional[int] = None):
        """
        Initialize a Partition for a specific topic.
        
        Args:
            topic: The topic name (e.g., "user_events")
            partition_id: The partition number (e.g., 0)
            data_dir: Base directory for all data
            segment_size_limit: Override default segment size limit (for testing)
        """
        self.topic = topic
        self.partition_id = partition_id
        self.base_data_dir = Path(data_dir)
        
        # Override segment size limit if provided (useful for testing)
        if segment_size_limit is not None:
            self.segment_size_limit = segment_size_limit
        else:
            self.segment_size_limit = self.SEGMENT_SIZE_LIMIT
        
        # Create partition-specific directory: data/{topic}-{partition_id}/
        self.partition_dir = self.base_data_dir / f"{topic}-{partition_id}"
        self.partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Segment management
        self.closed_segments: List[LogSegment] = []
        self.active_segment: Optional[LogSegment] = None
        self.next_offset = 0
        
        # Load existing segments from disk
        self._load_segments()
        
        # If no segments exist, create the first one
        if self.active_segment is None:
            self._create_new_segment(base_offset=0)
    
    def _load_segments(self):
        """
        Load existing segment files from disk.
        
        Scans the partition directory for .log files and loads them in order.
        The last segment becomes the active segment.
        """
        # Find all .log files in the partition directory
        log_files = sorted(self.partition_dir.glob("*.log"))
        
        if not log_files:
            return  # No existing segments
        
        # Load each segment
        for log_file in log_files:
            # Extract base offset from filename (e.g., "0000000000000000000.log" -> 0)
            base_offset = int(log_file.stem)
            
            # Create LogSegment instance
            segment = LogSegment(base_offset=base_offset, data_dir=str(self.partition_dir))
            
            # Add to closed segments (we'll move the last one to active later)
            self.closed_segments.append(segment)
            
            # Update next_offset to be after this segment's current offset
            if segment.current_offset > self.next_offset:
                self.next_offset = segment.current_offset
        
        # Make the last segment the active one
        if self.closed_segments:
            self.active_segment = self.closed_segments.pop()
            self.next_offset = self.active_segment.current_offset
    
    def _create_new_segment(self, base_offset: int):
        """
        Create a new segment and make it the active segment.
        
        Args:
            base_offset: The starting offset for this new segment
        """
        # Close the current active segment if it exists
        if self.active_segment is not None:
            self.closed_segments.append(self.active_segment)
        
        # Create new segment
        self.active_segment = LogSegment(
            base_offset=base_offset,
            data_dir=str(self.partition_dir)
        )
    
    def _should_roll_segment(self) -> bool:
        """
        Check if we should create a new segment (segment rolling).
        
        Returns:
            True if the active segment has exceeded the size limit
        """
        if self.active_segment is None:
            return False
        
        # Get current size of the log file
        log_file_path = self.partition_dir / f"{self.active_segment.base_offset:019d}.log"
        
        if not log_file_path.exists():
            return False
        
        current_size = log_file_path.stat().st_size
        return current_size >= self.segment_size_limit
    
    def produce(self, message: bytes) -> int:
        """
        Append a message to the partition.
        
        Handles segment rolling if the current segment is too large.
        
        Args:
            message: The raw message bytes to append
            
        Returns:
            The offset assigned to this message
        """
        # Check if we need to roll to a new segment
        if self._should_roll_segment():
            # Create new segment starting at the current offset
            self._create_new_segment(base_offset=self.next_offset)
        
        # Append to active segment
        offset = self.active_segment.append(message)
        
        # Update next offset
        self.next_offset = offset + 1
        
        return offset
    
    def consume(self, offset: int) -> bytes:
        """
        Read a message at the given offset.
        
        Searches through all segments to find the one containing the offset.
        
        Args:
            offset: The offset to read
            
        Returns:
            The message bytes at that offset
            
        Raises:
            ValueError: If offset is not found in any segment
        """
        # Check if offset is out of range
        if offset < 0 or offset >= self.next_offset:
            raise ValueError(
                f"Offset {offset} out of range [0, {self.next_offset})"
            )
        
        # First check closed segments (in order)
        for segment in self.closed_segments:
            if segment.base_offset <= offset < segment.current_offset:
                return segment.read(offset)
        
        # Check active segment
        if self.active_segment is not None:
            if self.active_segment.base_offset <= offset < self.active_segment.current_offset:
                return self.active_segment.read(offset)
        
        raise ValueError(f"Offset {offset} not found in any segment")
    
    def get_segment_count(self) -> int:
        """
        Get the total number of segments (active + closed).
        
        Returns:
            Total segment count
        """
        count = len(self.closed_segments)
        if self.active_segment is not None:
            count += 1
        return count
    
    def get_segment_info(self) -> dict:
        """
        Get information about all segments.
        
        Returns:
            Dictionary with segment details
        """
        info = {
            "topic": self.topic,
            "partition_id": self.partition_id,
            "total_segments": self.get_segment_count(),
            "next_offset": self.next_offset,
            "segments": []
        }
        
        # Add closed segments
        for segment in self.closed_segments:
            log_file = self.partition_dir / f"{segment.base_offset:019d}.log"
            size = log_file.stat().st_size if log_file.exists() else 0
            
            info["segments"].append({
                "base_offset": segment.base_offset,
                "current_offset": segment.current_offset,
                "size_bytes": size,
                "status": "closed"
            })
        
        # Add active segment
        if self.active_segment is not None:
            log_file = self.partition_dir / f"{self.active_segment.base_offset:019d}.log"
            size = log_file.stat().st_size if log_file.exists() else 0
            
            info["segments"].append({
                "base_offset": self.active_segment.base_offset,
                "current_offset": self.active_segment.current_offset,
                "size_bytes": size,
                "status": "active"
            })
        
        return info
    
    def close(self):
        """Close all segments."""
        for segment in self.closed_segments:
            segment.close()
        
        if self.active_segment is not None:
            self.active_segment.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures all segments are closed."""
        self.close()
        return False
    
    def __repr__(self):
        return (
            f"Partition(topic='{self.topic}', partition_id={self.partition_id}, "
            f"segments={self.get_segment_count()}, next_offset={self.next_offset})"
        )
