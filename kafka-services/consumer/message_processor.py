import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class MessageProcessor:
    def __init__(self):
        self.processed_count = 0
        self.error_count = 0
    
    async def process_chat_message(self, message: Dict[str, Any]):
        """Process chat messages from Kafka"""
        try:
            user_id = message.get("user_id")
            user_message = message.get("message")
            memory_type = message.get("memory_type")
            timestamp = message.get("timestamp")
            
            logger.info(f"Processing chat message from user {user_id}: {user_message[:50]}...")
            
            # Here you can add additional processing logic
            # For example: sentiment analysis, message categorization, etc.
            
            self.processed_count += 1
            logger.info(f"Successfully processed message. Total: {self.processed_count}")
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing chat message: {e}")
    
    async def process_comment_message(self, message: Dict[str, Any]):
        """Process comment messages from Kafka"""
        try:
            comment_id = message.get("commentId")
            text = message.get("text")
            category = message.get("category")
            sentiment = message.get("sentiment")
            
            logger.info(f"Processing comment {comment_id}: {text[:50]}...")
            
            # Here you can add comment processing logic
            # For example: sentiment analysis, category classification, etc.
            
            self.processed_count += 1
            logger.info(f"Successfully processed comment. Total: {self.processed_count}")
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing comment message: {e}")
    
    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics"""
        return {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "success_rate": (self.processed_count / (self.processed_count + self.error_count)) * 100 if (self.processed_count + self.error_count) > 0 else 0
        }