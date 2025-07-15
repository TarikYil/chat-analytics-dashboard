#!/usr/bin/env python3
"""
Kafka Consumer - Yorum İşleyici
Kafka'dan gelen yorumları işler ve veritabanına kaydeder
"""

import logging
import sys
import os
import threading
import time
from flask import Flask, jsonify

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KafkaConfig
from comment_processor import CommentProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)

# Global processor instance
processor = None
consumer_thread = None
consumer_running = False

@app.route('/health')
def health():
    """Health check endpoint"""
    global consumer_running
    try:
        if consumer_running and processor:
            return jsonify({
                "status": "healthy",
                "service": "kafka-consumer",
                "timestamp": time.time(),
                "consumer_running": consumer_running
            }), 200
        else:
            return jsonify({
                "status": "unhealthy",
                "service": "kafka-consumer",
                "error": "Consumer not running"
            }), 503
    except Exception as e:
        return jsonify({
            "status": "error",
            "service": "kafka-consumer",
            "error": str(e)
        }), 500

@app.route('/')
def root():
    """Root endpoint"""
    return jsonify({
        "service": "kafka-consumer",
        "status": "running",
        "endpoints": ["/health"]
    })

def run_consumer():
    """Consumer'ı ayrı thread'de çalıştır"""
    global processor, consumer_running
    try:
        logger.info("Starting consumer thread...")
        consumer_running = True
        processor.run_consumer()
    except Exception as e:
        logger.error(f"Error in consumer thread: {e}")
        consumer_running = False
    finally:
        consumer_running = False
        logger.info("Consumer thread stopped")

def main():
    """Ana fonksiyon - Yorum işleyici ve web server"""
    global processor, consumer_thread
    
    logger.info("Starting Kafka Consumer Service...")
    
    try:
        # Kafka config'i yükle
        kafka_config = KafkaConfig()
        logger.info(f"Using Kafka bootstrap servers: {kafka_config.bootstrap_servers}")
        
        # Yorum işleyici oluştur
        processor = CommentProcessor()
        
        # Consumer'ı ayrı thread'de başlat
        consumer_thread = threading.Thread(target=run_consumer, daemon=True)
        consumer_thread.start()
        
        logger.info("Starting Flask web server on port 8002...")
        # Flask server'ı başlat
        app.run(host='0.0.0.0', port=8002, debug=False)
        
    except KeyboardInterrupt:
        logger.info("Kafka Consumer Service stopped by user")
    except Exception as e:
        logger.error(f"Error in Kafka Consumer Service: {e}")
    finally:
        if processor:
            processor.close()
        logger.info("Kafka Consumer Service shutdown complete")

if __name__ == "__main__":
    main() 