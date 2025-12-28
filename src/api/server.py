"""
StreamLog API Server - FastAPI-based message broker API.

Provides HTTP endpoints for producing and consuming messages from topics.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from storage.partition import Partition


# Pydantic models for request/response validation
class ProduceRequest(BaseModel):
    """Request model for producing a message."""
    topic: str
    message: str


class ProduceResponse(BaseModel):
    """Response model for produce operation."""
    topic: str
    partition: int
    offset: int


class ConsumeResponse(BaseModel):
    """Response model for consume operation."""
    topic: str
    partition: int
    offset: int
    message: str


class PartitionInfo(BaseModel):
    """Information about a partition."""
    topic: str
    partition_id: int
    total_segments: int
    next_offset: int


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    topics: int
    partitions: int


# Initialize FastAPI app
app = FastAPI(
    title="StreamLog API",
    description="A Kafka-like distributed commit log system",
    version="2.0.0"
)


# Global state: Topic management
# In production, this would be a proper TopicManager class with persistence
topics: Dict[str, Partition] = {}


def get_or_create_partition(topic: str, partition_id: int = 0) -> Partition:
    """
    Get an existing partition or create a new one.
    
    Args:
        topic: Topic name
        partition_id: Partition ID (default: 0)
        
    Returns:
        Partition instance
    """
    key = f"{topic}-{partition_id}"
    
    if key not in topics:
        # Create new partition with smaller segment size for testing
        # In production, this would be configurable
        topics[key] = Partition(topic, partition_id, segment_size_limit=10 * 1024)  # 10KB for easy testing
    
    return topics[key]


@app.get("/", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        System health status
    """
    return HealthResponse(
        status="healthy",
        topics=len(set(p.topic for p in topics.values())),
        partitions=len(topics)
    )


@app.post("/produce", response_model=ProduceResponse)
async def produce(request: ProduceRequest):
    """
    Produce a message to a topic.
    
    Args:
        request: ProduceRequest with topic and message
        
    Returns:
        ProduceResponse with assigned offset
        
    Example:
        POST /produce
        {
            "topic": "events",
            "message": "Hello World"
        }
        
        Response:
        {
            "topic": "events",
            "partition": 0,
            "offset": 42
        }
    """
    try:
        # Get or create partition
        partition = get_or_create_partition(request.topic)
        
        # Convert message to bytes and produce
        message_bytes = request.message.encode('utf-8')
        offset = partition.produce(message_bytes)
        
        return ProduceResponse(
            topic=request.topic,
            partition=0,
            offset=offset
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce message: {str(e)}")


@app.get("/consume", response_model=ConsumeResponse)
async def consume(topic: str, offset: int, partition: int = 0):
    """
    Consume a message from a topic at a specific offset.
    
    Args:
        topic: Topic name
        offset: Offset to read from
        partition: Partition ID (default: 0)
        
    Returns:
        ConsumeResponse with the message
        
    Example:
        GET /consume?topic=events&offset=42
        
        Response:
        {
            "topic": "events",
            "partition": 0,
            "offset": 42,
            "message": "Hello World"
        }
    """
    try:
        # Get partition
        key = f"{topic}-{partition}"
        
        if key not in topics:
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
        
        partition_obj = topics[key]
        
        # Consume message
        message_bytes = partition_obj.consume(offset)
        message = message_bytes.decode('utf-8')
        
        return ConsumeResponse(
            topic=topic,
            partition=partition,
            offset=offset,
            message=message
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to consume message: {str(e)}")


@app.get("/topics/{topic}/info", response_model=dict)
async def get_topic_info(topic: str, partition: int = 0):
    """
    Get information about a topic-partition.
    
    Args:
        topic: Topic name
        partition: Partition ID (default: 0)
        
    Returns:
        Detailed partition information including segment details
    """
    key = f"{topic}-{partition}"
    
    if key not in topics:
        raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
    
    partition_obj = topics[key]
    return partition_obj.get_segment_info()


@app.get("/topics")
async def list_topics():
    """
    List all topics.
    
    Returns:
        List of topic names
    """
    unique_topics = list(set(p.topic for p in topics.values()))
    return {"topics": unique_topics, "count": len(unique_topics)}


@app.delete("/topics/{topic}")
async def delete_topic(topic: str):
    """
    Delete a topic (close all partitions).
    
    Note: This doesn't delete the data files, just closes the partitions.
    
    Args:
        topic: Topic name
    """
    keys_to_delete = [k for k in topics.keys() if k.startswith(f"{topic}-")]
    
    if not keys_to_delete:
        raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
    
    # Close all partitions for this topic
    for key in keys_to_delete:
        topics[key].close()
        del topics[key]
    
    return {"message": f"Topic '{topic}' closed", "partitions_closed": len(keys_to_delete)}


# Cleanup on shutdown
@app.on_event("shutdown")
async def shutdown_event():
    """Close all partitions on shutdown."""
    for partition in topics.values():
        partition.close()


# For running the server directly
if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("StreamLog API Server")
    print("=" * 60)
    print("Starting server on http://localhost:8000")
    print("API Docs: http://localhost:8000/docs")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
