import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict, Any, List
import os
from fastapi import FastAPI, HTTPException
import uvicorn
from datetime import datetime
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Kafka Consumer API")

class KafkaConsumerService:
    """Kafka consumer service for message consumption"""
    
    def __init__(self):
        self.consumer = None
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.is_connected = False
        self.consumed_messages = []
        self.max_messages = 1000  # Keep last 1000 messages in memory
        
    def connect(self, topic: str, group_id: str = "default-group"):
        """Initialize Kafka consumer connection"""
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            self.is_connected = True
            logger.info(f"Kafka consumer connected successfully to topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {e}")
            self.is_connected = False
            raise
    
    def consume_messages(self, topic: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        """Consume messages from Kafka topic"""
        if not self.is_connected:
            self.connect(topic)
            
        messages = []
        try:
            for message in self.consumer:
                msg_data = {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": datetime.fromtimestamp(message.timestamp / 1000).isoformat(),
                    "value": message.value
                }
                messages.append(msg_data)
                
                # Store in memory for API access
                self.consumed_messages.append(msg_data)
                if len(self.consumed_messages) > self.max_messages:
                    self.consumed_messages.pop(0)
                
                if len(messages) >= max_messages:
                    break
                    
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        
        return messages
    
    def get_recent_messages(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent consumed messages"""
        return self.consumed_messages[-limit:] if self.consumed_messages else []
    
    def get_message_stats(self) -> Dict[str, Any]:
        """Get message consumption statistics"""
        if not self.consumed_messages:
            return {
                "total_messages": 0,
                "topics": {},
                "recent_activity": "No messages"
            }
        
        # Count messages by topic
        topic_counts = {}
        for msg in self.consumed_messages:
            topic = msg.get("topic", "unknown")
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
        
        # Get recent activity
        if self.consumed_messages:
            latest_msg = self.consumed_messages[-1]
            recent_activity = latest_msg.get("timestamp", "unknown")
        else:
            recent_activity = "No messages"
        
        return {
            "total_messages": len(self.consumed_messages),
            "topics": topic_counts,
            "recent_activity": recent_activity
        }
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.consumer.close()
            self.is_connected = False
            logger.info("Kafka consumer connection closed")
    
    def is_healthy(self) -> bool:
        """Check if consumer is healthy"""
        return self.is_connected

# Initialize consumer service
consumer_service = KafkaConsumerService()

@app.on_event("startup")
async def startup_event():
    """Initialize consumer on startup"""
    try:
        # Connect to default topic
        consumer_service.connect("raw-comments")
        logger.info("Consumer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize consumer: {e}")

@app.get("/messages")
async def get_messages(limit: int = 50):
    """Get recent consumed messages"""
    try:
        messages = consumer_service.get_recent_messages(limit)
        return {
            "status": "success",
            "data": messages,
            "count": len(messages)
        }
    except Exception as e:
        logger.error(f"Error getting messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/messages/stats")
async def get_message_stats():
    """Get message consumption statistics"""
    try:
        stats = consumer_service.get_message_stats()
        return {
            "status": "success",
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting message stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/consume")
async def consume_messages(topic: str, max_messages: int = 10):
    """Manually consume messages from topic"""
    try:
        messages = consumer_service.consume_messages(topic, max_messages)
        return {
            "status": "success",
            "data": messages,
            "count": len(messages)
        }
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if consumer_service.is_healthy() else "unhealthy",
        "service": "kafka-consumer",
        
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002) 