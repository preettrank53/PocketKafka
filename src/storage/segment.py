"""
LogSegment - A single segment of the distributed commit log.

Binary Format:
- Log File (.log):
  Each entry consists of:
  1. Length (4 bytes, big-endian unsigned int): Size of the message body
  2. Message Body (variable length): The actual message bytes
  
- Index File (.index):
  Each entry consists of:
  1. Relative Offset (4 bytes, big-endian unsigned int): Offset within segment
  2. File Position (4 bytes, big-endian unsigned int): Byte position in .log file
  
  Index entries are written every INDEX_INTERVAL bytes to enable fast lookups.
"""

import os
import struct
from pathlib import Path


class LogSegment:
    """
    Manages a single segment of the log (e.g., messages 0 to 1000).
    
    Each segment consists of two files:
    - .log file: Contains the actual message data
    - .index file: Contains offset -> file position mappings for fast lookups
    """
    
    # Write an index entry every 4KB to balance space vs lookup speed
    INDEX_INTERVAL = 4096
    
    def __init__(self, base_offset: int, data_dir: str = "data"):
        """
        Initialize a LogSegment.
        
        Args:
            base_offset: The starting offset ID for this segment (e.g., 0, 1000, 2000)
            data_dir: Directory where log and index files will be stored
        """
        self.base_offset = base_offset
        self.data_dir = Path(data_dir)
        self.current_offset = base_offset
        self.bytes_since_last_index = 0
        
        # Create data directory if it doesn't exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate file names (e.g., 0000000000000000000.log)
        segment_name = f"{base_offset:019d}"
        log_path = self.data_dir / f"{segment_name}.log"
        index_path = self.data_dir / f"{segment_name}.index"
        
        # Open files in append+binary mode ('ab+' allows read and append)
        self.log_file = open(log_path, 'ab+')
        self.index_file = open(index_path, 'ab+')
        
        # Track the current position in the log file
        self.log_file.seek(0, os.SEEK_END)
        self.last_indexed_position = 0
        
        # Recover current offset by scanning the log file
        self._recover_offset()
    
    def _recover_offset(self):
        """
        Recover the current offset by scanning the log file.
        
        This is needed when loading an existing segment from disk to determine
        where writing should resume.
        """
        # Seek to beginning of log file
        self.log_file.seek(0)
        
        # Count messages in the file
        message_count = 0
        
        while True:
            # Try to read length prefix
            length_bytes = self.log_file.read(4)
            
            if len(length_bytes) < 4:
                # End of file or corrupted
                break
            
            # Unpack length
            message_length = struct.unpack(">I", length_bytes)[0]
            
            # Skip over the message body
            self.log_file.seek(message_length, os.SEEK_CUR)
            
            message_count += 1
        
        # Update current offset
        self.current_offset = self.base_offset + message_count
        
        # Seek back to end for appending
        self.log_file.seek(0, os.SEEK_END)
    
    def append(self, message: bytes) -> int:
        """
        Append a message to the log segment.
        
        Binary Format Written:
        - 4 bytes: Message length (big-endian unsigned int)
        - N bytes: Message body
        
        Args:
            message: The raw message bytes to append
            
        Returns:
            The offset assigned to this message
        """
        # Get current file position before writing
        file_position = self.log_file.tell()
        
        # Calculate message length
        message_length = len(message)
        
        # Pack length as 4-byte big-endian unsigned integer (">I")
        # ">" = big-endian (network byte order)
        # "I" = unsigned int (4 bytes)
        length_bytes = struct.pack(">I", message_length)
        
        # Write length prefix
        self.log_file.write(length_bytes)
        
        # Write message body
        self.log_file.write(message)
        
        # Flush to ensure data is written to disk
        self.log_file.flush()
        
        # Track bytes written
        bytes_written = 4 + message_length
        self.bytes_since_last_index += bytes_written
        
        # Write index entry every INDEX_INTERVAL bytes
        if self.bytes_since_last_index >= self.INDEX_INTERVAL:
            self._write_index_entry(self.current_offset, file_position)
            self.bytes_since_last_index = 0
        
        # Assign offset to this message and increment
        assigned_offset = self.current_offset
        self.current_offset += 1
        
        return assigned_offset
    
    def _write_index_entry(self, offset: int, file_position: int):
        """
        Write an index entry to the index file.
        
        Args:
            offset: The absolute offset
            file_position: The byte position in the log file
        """
        # Calculate relative offset (offset within this segment)
        relative_offset = offset - self.base_offset
        
        # Pack both as 4-byte big-endian unsigned integers
        index_entry = struct.pack(">II", relative_offset, file_position)
        
        # Write to index file
        self.index_file.write(index_entry)
        self.index_file.flush()
        
        self.last_indexed_position = file_position
    
    def read(self, offset: int) -> bytes:
        """
        Read a message at the given offset.
        
        This is a simple linear scan implementation. In production, we would
        use the index file to jump close to the target offset, then scan.
        
        Args:
            offset: The absolute offset to read
            
        Returns:
            The message bytes at that offset
            
        Raises:
            ValueError: If offset is out of range or not found
        """
        if offset < self.base_offset or offset >= self.current_offset:
            raise ValueError(
                f"Offset {offset} out of range [{self.base_offset}, {self.current_offset})"
            )
        
        # Seek to beginning of log file
        self.log_file.seek(0)
        
        # Scan through messages to find the target offset
        current_scan_offset = self.base_offset
        
        while current_scan_offset <= offset:
            # Read length prefix (4 bytes)
            length_bytes = self.log_file.read(4)
            
            if len(length_bytes) < 4:
                raise ValueError(f"Offset {offset} not found (corrupted log)")
            
            # Unpack length
            message_length = struct.unpack(">I", length_bytes)[0]
            
            # Read message body
            message = self.log_file.read(message_length)
            
            if len(message) < message_length:
                raise ValueError(f"Offset {offset} not found (corrupted log)")
            
            # If this is the target offset, return it
            if current_scan_offset == offset:
                return message
            
            # Move to next message
            current_scan_offset += 1
        
        raise ValueError(f"Offset {offset} not found")
    
    def close(self):
        """Close the log and index files."""
        if self.log_file:
            self.log_file.close()
        if self.index_file:
            self.index_file.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures files are closed."""
        self.close()
        return False
    
    def __repr__(self):
        return (
            f"LogSegment(base_offset={self.base_offset}, "
            f"current_offset={self.current_offset}, "
            f"data_dir='{self.data_dir}')"
        )
