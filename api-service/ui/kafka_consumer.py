import json
import logging
import threading
import time
from typing import Dict, List, Any, Callable
from kafka import KafkaConsumer
from collections import deque
import streamlit as st

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamlitKafkaConsumer:
    def __init__(self, bootstrap_servers: str = 'kafka:9092'):
        """Initialize Kafka consumer for Streamlit"""
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.is_running = False
        self.messages = deque(maxlen=1000)  # Keep last 1000 messages
        self.sentiment_messages = deque(maxlen=100)
        self.category_messages = deque(maxlen=100)
        self.trend_messages = deque(maxlen=100)
        self.keyword_messages = deque(maxlen=100)
        self.realtime_messages = deque(maxlen=100)
        self.callbacks = []
        self.thread = None
        
        # Initialize consumer
        self._init_consumer()
    
    def _init_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'sentiment-analytics', 'category-analytics', 'trend-analytics', 
                'real-time-analytics', 'analytics-results',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='streamlit-consumer-group-v4',  # Changed group ID
                auto_offset_reset='latest',  # Changed to latest
                enable_auto_commit=True,
                consumer_timeout_ms=5000,  # Increased timeout to 5 seconds
                session_timeout_ms=30000,  # Added session timeout
                heartbeat_interval_ms=10000  # Added heartbeat
            )
            logger.info("Kafka consumer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            self.consumer = None
    
    def add_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Add callback function to be called when new message arrives"""
        self.callbacks.append(callback)
    
    def start_consuming(self):
        """Start consuming messages in a separate thread"""
        if self.is_running:
            logger.warning("Consumer is already running")
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._consume_messages, daemon=True)
        self.thread.start()
        logger.info("Started consuming messages")
    
    def stop_consuming(self):
        """Stop consuming messages"""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Stopped consuming messages")
    
    def _consume_messages(self):
        """Consume messages from Kafka"""
        if not self.consumer:
            logger.error("Consumer not initialized")
            return
        
        logger.info("Starting to consume messages from Kafka...")
        
        try:
            while self.is_running:
                try:
                    # Use poll instead of for loop for better control
                    message_batch = self.consumer.poll(timeout_ms=2000, max_records=10)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.is_running:
                                break
                            
                            try:
                                # Parse message
                                data = message.value
                                timestamp = time.time()
                                
                                # Add timestamp if not present
                                if 'timestamp' not in data:
                                    data['timestamp'] = timestamp
                                
                                # Add to appropriate message queue based on topic
                                topic = message.topic
                                if topic == 'sentiment-analytics':
                                    self.sentiment_messages.append(data)
                                    logger.info(f"Received sentiment analytics: {data.get('timestamp', 'unknown')}")
                                elif topic == 'category-analytics':
                                    self.category_messages.append(data)
                                    logger.info(f"Received category analytics: {data.get('timestamp', 'unknown')}")
                                elif topic == 'trend-analytics':
                                    self.trend_messages.append(data)
                                    logger.info(f"Received trend analytics: {data.get('timestamp', 'unknown')}")
                                elif topic == 'real-time-analytics':
                                    self.realtime_messages.append(data)
                                    logger.info(f"Received real-time analytics: {data.get('timestamp', 'unknown')}")
                                else:
                                    self.messages.append(data)
                                    logger.info(f"Received message from {topic}: {data.get('timestamp', 'unknown')}")
                                
                                # Call callbacks
                                for callback in self.callbacks:
                                    try:
                                        callback(data)
                                    except Exception as e:
                                        logger.error(f"Callback error: {e}")
                                
                            except Exception as e:
                                logger.error(f"Error processing message: {e}")
                                continue
                    
                    # Small delay to prevent busy waiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error in message consumption loop: {e}")
                    time.sleep(1)  # Wait before retrying
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Consumer loop ended")
    
    def get_latest_messages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest messages from queue"""
        return list(self.messages)[-limit:]
    
    def get_sentiment_messages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest sentiment analytics messages"""
        return list(self.sentiment_messages)[-limit:]
    
    def get_category_messages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest category analytics messages"""
        return list(self.category_messages)[-limit:]
    
    def get_trend_messages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest trend analytics messages"""
        return list(self.trend_messages)[-limit:]
    
    def get_keyword_messages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest keyword analytics messages"""
        return list(self.keyword_messages)[-limit:]
    
    def get_realtime_messages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest real-time analytics messages"""
        return list(self.realtime_messages)[-limit:]
    
    def get_message_count(self) -> int:
        """Get total message count"""
        return len(self.messages)
    
    def clear_messages(self):
        """Clear all message queues"""
        self.messages.clear()
        self.sentiment_messages.clear()
        self.category_messages.clear()
        self.trend_messages.clear()
        self.keyword_messages.clear()
        self.realtime_messages.clear()
    
    def get_analytics_data(self) -> Dict[str, Any]:
        """Get aggregated analytics data from latest messages"""
        analytics_data = {
            'sentiment_analytics': {},
            'category_analytics': {},
            'trend_analytics': {},
            'keyword_analytics': {},
            'real_time_analytics': {},
            'latest_timestamp': None
        }
        
        # Get latest sentiment analytics
        if self.sentiment_messages:
            latest_sentiment = list(self.sentiment_messages)[-1]
            analytics_data['sentiment_analytics'] = latest_sentiment.get('data', {})
            analytics_data['latest_timestamp'] = latest_sentiment.get('timestamp')
        
        # Get latest category analytics
        if self.category_messages:
            latest_category = list(self.category_messages)[-1]
            analytics_data['category_analytics'] = latest_category.get('data', {})
        
        # Get latest trend analytics
        if self.trend_messages:
            latest_trend = list(self.trend_messages)[-1]
            analytics_data['trend_analytics'] = latest_trend.get('data', {})
        
        # Get latest keyword analytics
        if self.keyword_messages:
            latest_keyword = list(self.keyword_messages)[-1]
            analytics_data['keyword_analytics'] = latest_keyword.get('data', {})
        
        # Get latest real-time analytics
        if self.realtime_messages:
            latest_realtime = list(self.realtime_messages)[-1]
            analytics_data['real_time_analytics'] = latest_realtime.get('data', {})
        
        return analytics_data

# Global consumer instance
consumer_instance = None

def get_kafka_consumer() -> StreamlitKafkaConsumer:
    """Get or create Kafka consumer instance"""
    global consumer_instance
    if consumer_instance is None:
        consumer_instance = StreamlitKafkaConsumer()
    return consumer_instance

def start_kafka_consumer():
    """Start Kafka consumer"""
    consumer = get_kafka_consumer()
    consumer.start_consuming()
    return consumer

def stop_kafka_consumer():
    """Stop Kafka consumer"""
    global consumer_instance
    if consumer_instance:
        consumer_instance.stop_consuming()
        consumer_instance = None

# Callback function for Streamlit
def on_message_received(data: Dict[str, Any]):
    """Callback function called when new message arrives"""
    logger.info(f"New analytics data received: {data.get('timestamp', 'unknown')}")
    
    # Update Streamlit session state
    if 'kafka_messages' not in st.session_state:
        st.session_state.kafka_messages = []
    
    st.session_state.kafka_messages.append(data)
    
    # Keep only last 100 messages
    if len(st.session_state.kafka_messages) > 100:
        st.session_state.kafka_messages = st.session_state.kafka_messages[-100:]
    
    # Update last update time
    st.session_state.last_update = time.time()

# Test function
def test_kafka_consumer():
    """Test Kafka consumer"""
    consumer = StreamlitKafkaConsumer()
    consumer.add_callback(on_message_received)
    consumer.start_consuming()
    
    try:
        time.sleep(10)  # Listen for 10 seconds
        messages = consumer.get_latest_messages()
        print(f"Received {len(messages)} messages")
        
        analytics_data = consumer.get_analytics_data()
        print(f"Analytics data: {analytics_data}")
        
    finally:
        consumer.stop_consuming()

if __name__ == '__main__':
    test_kafka_consumer() 