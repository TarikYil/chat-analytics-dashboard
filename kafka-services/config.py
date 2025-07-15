import os
from typing import Dict, Any

class KafkaConfig:
    """Kafka configuration management"""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.topics = {
            'raw_comments': 'raw-comments',
            'chat_messages': 'chat-messages',
            'processed_comments': 'processed-comments'
        }
        self.producer_config = {
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 1,
            'buffer_memory': 33554432
        }
        self.consumer_config = {
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'ai-chatbot-group'
        }
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration"""
        return {
            'bootstrap_servers': self.bootstrap_servers,
            **self.producer_config
        }
    
    def get_consumer_config(self, group_id: str = None) -> Dict[str, Any]:
        """Get consumer configuration"""
        config = {
            'bootstrap_servers': self.bootstrap_servers,
            **self.consumer_config
        }
        if group_id:
            config['group_id'] = group_id
        return config
    
    def get_topic(self, topic_name: str) -> str:
        """Get topic name"""
        return self.topics.get(topic_name, topic_name) 