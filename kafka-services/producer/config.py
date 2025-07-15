import os
from typing import Dict, Any

class KafkaConfig:
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