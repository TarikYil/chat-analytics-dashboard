import json
import logging
from datetime import datetime
from typing import Dict, Any
import psycopg2
import os
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import grpc
import sys
import os

# Add gRPC protobuf path
sys.path.append('/app/grpc-sentiment-service/proto')
try:
    import sentiment_pb2
    import sentiment_pb2_grpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    sentiment_pb2 = None
    sentiment_pb2_grpc = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CommentProcessor:
    """Kafka'dan gelen yorumları işleyen servis"""
    
    def __init__(self):
        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'chatbot_db'),
            'user': os.getenv('POSTGRES_USER', 'chatbot_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'chatbot_pass')
        }
        
        # Redis configuration
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            decode_responses=True
        )
        
        # Kafka consumer
        self.consumer = None
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        
        # Kafka producer for processed comments
        self.producer = None
        
        # gRPC client
        self.grpc_stub = None
        if GRPC_AVAILABLE:
            try:
                self.grpc_channel = grpc.insecure_channel('grpc-sentiment-service:50051')
                self.grpc_stub = sentiment_pb2_grpc.SentimentAnalysisStub(self.grpc_channel)
                logger.info("gRPC client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize gRPC client: {e}")
                self.grpc_stub = None
        
        # Statistics
        self.stats = {
            'processed': 0,
            'errors': 0,
            'grpc_calls': 0,
            'grpc_errors': 0,
            'last_processed': None
        }
    
    def connect_kafka(self):
        """Kafka consumer bağlantısı"""
        try:
            self.consumer = KafkaConsumer(
                'raw-comments',
                bootstrap_servers=self.bootstrap_servers,
                group_id='comment-processor-group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Kafka consumer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {e}")
            raise
    
    def get_db_connection(self):
        """Database bağlantısı"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def save_to_database(self, comment_data: Dict[str, Any]) -> bool:
        """Yorumu veritabanına kaydet"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Yorumu processed_comments tablosuna ekle
            cursor.execute("""
                INSERT INTO processed_comments 
                (commentId, message, sentiment, category, user_id, timestamp, language)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (commentId) DO UPDATE SET
                    message = EXCLUDED.message,
                    sentiment = EXCLUDED.sentiment,
                    category = EXCLUDED.category,
                    timestamp = CURRENT_TIMESTAMP
            """, (
                comment_data['commentId'],
                comment_data['text'],
                comment_data['sentiment'],
                comment_data['category'],
                comment_data.get('user_id', 'llm_generator'),
                comment_data['timestamp'],
                comment_data.get('language', 'tr')
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Comment saved to database: {comment_data['commentId']}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving comment to database: {e}")
            return False
    
    def update_cache(self, comment_data: Dict[str, Any]):
        """Redis cache'i güncelle"""
        try:
            # Son yorumları cache'de sakla
            cache_key = f"recent_comments:{comment_data['sentiment']}"
            # Cache'e ekle (son 50 yorum)
            self.redis_client.lpush(cache_key, json.dumps(comment_data))
            self.redis_client.ltrim(cache_key, 0, 49)  # Sadece son 50'yi tut
            # İstatistikleri güncelle
            stats_key = f"stats:{comment_data['sentiment']}"
            self.redis_client.incr(stats_key)
            # Toplam istatistik
            self.redis_client.incr("stats:total")
            logger.info(f"Cache updated for comment: {comment_data['commentId']}")

            # Son N sentiment yorumunu ayrı bir anahtarda tut
            try:
                sentiment = comment_data['sentiment']
                comment_text = comment_data['text']
                key = f"last_{sentiment}_comments"
                self.redis_client.lpush(key, comment_text)
                self.redis_client.ltrim(key, 0, 9)  # Son 10 yorum
            except Exception as e:
                logger.error(f"Error updating last_{sentiment}_comments in Redis: {e}")

        except Exception as e:
            logger.error(f"Error updating cache: {e}")
    
    def process_comment(self, comment_data: Dict[str, Any]) -> bool:
        """Yorum işleme"""
        try:
            # Sentiment analizi yap
            sentiment_result = self.analyze_sentiment_grpc(comment_data['text'])
            
            # İşlenmiş yorum verisi oluştur
            processed_comment = {
                'commentId': comment_data['commentId'],
                'text': comment_data['text'],
                'category': comment_data['category'],
                'sentiment': sentiment_result['sentiment'],
                'confidence': sentiment_result['confidence'],
                'timestamp': comment_data['timestamp'],
                'language': comment_data.get('language', 'tr'),
                'source': sentiment_result['source'],
                'processing_time': sentiment_result.get('processing_time', 0),
                'original_sentiment': comment_data.get('sentiment', 'unknown')
            }
            
            # Veritabanına kaydet
            db_success = self.save_to_database(processed_comment)
            
            # Cache'i güncelle
            self.update_cache(processed_comment)
            
            # processed-comments topic'ine gönder
            kafka_success = self.send_to_processed_topic(processed_comment)
            
            # İstatistikleri güncelle
            self.stats['processed'] += 1
            self.stats['last_processed'] = datetime.now().isoformat()
            
            if db_success and kafka_success:
                logger.info(f"Comment processed successfully: {comment_data['commentId']}")
                return True
            else:
                self.stats['errors'] += 1
                logger.error(f"Failed to process comment: {comment_data['commentId']}")
                return False
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error processing comment: {e}")
            return False
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """İşleme istatistiklerini al"""
        try:
            # Redis'den istatistikleri al
            total = self.redis_client.get("stats:total") or 0
            positive = self.redis_client.get("stats:positive") or 0
            negative = self.redis_client.get("stats:negative") or 0
            neutral = self.redis_client.get("stats:neutral") or 0
            
            return {
                "total_processed": int(total),
                "positive": int(positive),
                "negative": int(negative),
                "neutral": int(neutral),
                "errors": self.stats['errors'],
                "grpc_calls": self.stats['grpc_calls'],
                "grpc_errors": self.stats['grpc_errors'],
                "last_processed": self.stats['last_processed']
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return self.stats
    
    def run_consumer(self):
        """Kafka consumer'ı çalıştır"""
        if not self.consumer:
            self.connect_kafka()
        
        logger.info("Starting comment processor...")
        
        try:
            for message in self.consumer:
                try:
                    comment_data = message.value
                    logger.info(f"Received comment: {comment_data.get('commentId', 'unknown')}")
                    
                    # Yorumu işle
                    success = self.process_comment(comment_data)
                    
                    if success:
                        logger.info(f"Comment processed: {comment_data.get('commentId', 'unknown')}")
                    else:
                        logger.error(f"Failed to process comment: {comment_data.get('commentId', 'unknown')}")
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.stats['errors'] += 1
                    
        except KeyboardInterrupt:
            logger.info("Comment processor stopped by user")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.close()
    
    def close(self):
        """Bağlantıları kapat"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")

    def analyze_sentiment_grpc(self, text: str) -> Dict[str, Any]:
        """gRPC servisi ile sentiment analizi yap"""
        if not GRPC_AVAILABLE or not self.grpc_stub:
            logger.warning("gRPC not available, using fallback sentiment")
            return {"sentiment": "neutral", "confidence": 0.5, "source": "fallback"}
        
        try:
            self.stats['grpc_calls'] += 1
            
            # gRPC request oluştur
            request = sentiment_pb2.SentimentRequest(
                text=text,
                language="tr",
                request_id=f"consumer_{int(datetime.now().timestamp() * 1000)}"
            )
            
            # gRPC çağrısı yap
            response = self.grpc_stub.AnalyzeSentiment(request)
            
            logger.info(f"gRPC sentiment analysis successful: {response.sentiment}")
            
            return {
                "sentiment": response.sentiment,
                "confidence": response.confidence,
                "processing_time": response.processing_time,
                "source": "grpc"
            }
            
        except grpc.RpcError as e:
            self.stats['grpc_errors'] += 1
            logger.error(f"gRPC error: {e}")
            return {"sentiment": "neutral", "confidence": 0.5, "source": "fallback"}
        except Exception as e:
            self.stats['grpc_errors'] += 1
            logger.error(f"Error in sentiment analysis: {e}")
            return {"sentiment": "neutral", "confidence": 0.5, "source": "fallback"}
    
    def connect_kafka_producer(self):
        """Kafka producer bağlantısı"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Kafka producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {e}")
            raise
    
    def send_to_processed_topic(self, processed_comment: Dict[str, Any]) -> bool:
        """İşlenmiş yorumu processed-comments topic'ine gönder"""
        try:
            if not self.producer:
                self.connect_kafka_producer()
            
            future = self.producer.send('processed-comments', processed_comment)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Processed comment sent to Kafka: {processed_comment['commentId']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send processed comment to Kafka: {e}")
            return False

def main():
    """Ana fonksiyon"""
    processor = CommentProcessor()
    
    try:
        processor.run_consumer()
    except KeyboardInterrupt:
        logger.info("Comment processor stopped")
    finally:
        processor.close()

if __name__ == "__main__":
    main() 