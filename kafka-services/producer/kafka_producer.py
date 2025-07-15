import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Dict, Any
import os
from fastapi import FastAPI, HTTPException
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Kafka Producer API")

class KafkaProducerService:
    """Kafka producer service for message publishing"""
    
    def __init__(self):
        self.producer = None
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.is_connected = False
        
    def connect(self):
        """Initialize Kafka producer connection"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=1,
                buffer_memory=33554432
            )
            self.is_connected = True
            logger.info("Kafka producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {e}")
            self.is_connected = False
            raise
    
    def send_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """Send message to Kafka topic"""
        if not self.is_connected:
            self.connect()
            
        try:
            future = self.producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            logger.info(f"Message sent to {topic} partition {record_metadata.partition}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def send_batch(self, topic: str, messages: list) -> Dict[str, int]:
        """Send multiple messages to Kafka topic"""
        if not self.is_connected:
            self.connect()
            
        success_count = 0
        error_count = 0
        
        for message in messages:
            if self.send_message(topic, message):
                success_count += 1
            else:
                error_count += 1
        
        logger.info(f"Batch sent: {success_count} success, {error_count} errors")
        return {"success": success_count, "errors": error_count}
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            self.is_connected = False
            logger.info("Kafka producer connection closed")
    
    def is_healthy(self) -> bool:
        """Check if producer is healthy"""
        return self.is_connected

# Initialize producer service
producer_service = KafkaProducerService()

@app.on_event("startup")
async def startup_event():
    """Initialize producer on startup"""
    try:
        producer_service.connect()
    except Exception as e:
        logger.error(f"Failed to initialize producer: {e}")

@app.post("/send")
async def send_message(topic: str, message: Dict[str, Any]):
    """Send message to Kafka topic"""
    try:
        success = producer_service.send_message(topic, message)
        if success:
            return {"status": "success", "message": "Message sent successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to send message")
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/send/batch")
async def send_batch(topic: str, messages: list):
    """Send batch of messages to Kafka topic"""
    try:
        result = producer_service.send_batch(topic, messages)
        return {
            "status": "success",
            "result": result
        }
    except Exception as e:
        logger.error(f"Error sending batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if producer_service.is_healthy() else "unhealthy",
        "service": "kafka-producer"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001) 