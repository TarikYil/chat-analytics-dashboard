import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict, Any, Optional
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.producer = None
        self.consumers = {}
    
    def get_producer(self) -> KafkaProducer:
        """Get or create Kafka producer"""
        if not self.producer:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("Kafka producer created successfully")
            except Exception as e:
                logger.error(f"Failed to create producer: {e}")
                raise
        return self.producer
    
    def get_consumer(self, topic: str, group_id: str) -> KafkaConsumer:
        """Get or create Kafka consumer"""
        consumer_key = f"{topic}_{group_id}"
        
        if consumer_key not in self.consumers:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                self.consumers[consumer_key] = consumer
                logger.info(f"Kafka consumer created for topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to create consumer: {e}")
                raise
        
        return self.consumers[consumer_key]
    
    def send_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """Send message to Kafka topic"""
        try:
            producer = self.get_producer()
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            logger.info(f"Message sent to {topic} partition {record_metadata.partition}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def close(self):
        """Close all connections"""
        if self.producer:
            self.producer.close()
        
        for consumer in self.consumers.values():
            consumer.close()
        
        logger.info("All Kafka connections closed") 