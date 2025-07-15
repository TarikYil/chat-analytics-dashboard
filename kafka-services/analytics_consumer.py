import json
import logging
import time
import redis
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any
import grpc
import analytics_pb2
import analytics_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsConsumer:
    def __init__(self):
        """Initialize analytics consumer"""
        self.kafka_bootstrap_servers = 'kafka:9092'
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        
        # Database configuration
        self.db_config = {
            'host': 'postgres',
            'database': 'chatbot_db',
            'user': 'chatbot_user',
            'password': 'chatbot_pass'
        }
        
        # Analytics gRPC client
        self.analytics_client = None
        self._init_analytics_client()
        
        # Initialize Kafka consumer and producer
        self._init_kafka()
        self._init_database()
    
    def _init_analytics_client(self):
        """Initialize analytics gRPC client"""
        try:
            self.analytics_client = analytics_pb2_grpc.AnalyticsServiceStub(
                grpc.insecure_channel('grpc-analytics-service:50052')
            )
            logger.info("Analytics gRPC client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize analytics client: {e}")
            self.analytics_client = None
    
    def _init_kafka(self):
        """Initialize Kafka consumer and producer"""
        try:
            # Consumer for processed comments
            self.consumer = KafkaConsumer(
                'processed-comments',
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='analytics-consumer-group',
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            
            # Producer for analytics results
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            logger.info("Kafka consumer and producer initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka: {e}")
            raise
    
    def _init_database(self):
        """Initialize database connection"""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            # Set autocommit to True to avoid transaction issues
            self.db_conn.autocommit = True
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self.db_conn = None
    
    def _ensure_db_connection(self):
        """Ensure database connection is available, reconnect if needed"""
        if self.db_conn is None:
            logger.info("Attempting to reconnect to database...")
            self._init_database()
        else:
            try:
                # Test connection
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
            except Exception as e:
                logger.warning(f"Database connection test failed: {e}")
                logger.info("Reconnecting to database...")
                try:
                    self.db_conn.close()
                except:
                    pass
                self.db_conn = None
                self._init_database()
        return self.db_conn is not None
    
    def update_real_time_metrics(self, comment_data: Dict[str, Any]):
        """Update real-time metrics in Redis"""
        try:
            # Update sentiment ratios
            sentiment = comment_data.get('sentiment', 'neutral')
            if sentiment == 'positive':
                self.redis_client.incr('positive_comments')
            elif sentiment == 'negative':
                self.redis_client.incr('negative_comments')
            else:
                self.redis_client.incr('neutral_comments')
            
            # Update category counts
            category = comment_data.get('category', 'unknown')
            self.redis_client.incr(f'category_{category}')
            
            # Update total comments
            self.redis_client.incr('total_comments')
            
            # Calculate ratios
            total = int(self.redis_client.get('total_comments') or 0)
            positive = int(self.redis_client.get('positive_comments') or 0)
            negative = int(self.redis_client.get('negative_comments') or 0)
            neutral = int(self.redis_client.get('neutral_comments') or 0)
            
            if total > 0:
                self.redis_client.set('positive_ratio', (positive / total) * 100)
                self.redis_client.set('negative_ratio', (negative / total) * 100)
                self.redis_client.set('neutral_ratio', (neutral / total) * 100)
            
            # Update comments per minute
            current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
            minute_key = f'comments_minute_{current_minute}'
            self.redis_client.incr(minute_key)
            self.redis_client.expire(minute_key, 120)  # Expire after 2 minutes
            
            # Calculate comments per minute
            comments_this_minute = int(self.redis_client.get(minute_key) or 0)
            self.redis_client.set('comments_per_minute', comments_this_minute)
            
            logger.info(f"Updated real-time metrics for comment: {comment_data.get('commentId')}")
            
        except Exception as e:
            logger.error(f"Error updating real-time metrics: {e}")
    
    def get_analytics_data(self, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get analytics data from gRPC service"""
        try:
            if not self.analytics_client:
                logger.warning("Analytics client not available")
                return {}
            
            # Get sentiment distribution
            sentiment_request = analytics_pb2.SentimentDistributionRequest(
                time_range="24h"
            )
            sentiment_response = self.analytics_client.GetSentimentDistribution(sentiment_request)
            
            # Get category distribution
            category_request = analytics_pb2.CategoryDistributionRequest(
                time_range="24h"
            )
            category_response = self.analytics_client.GetCategoryDistribution(category_request)
            
            # Get time series data (trend analytics)
            time_series_request = analytics_pb2.TimeSeriesRequest(
                time_range="24h",
                interval="1h",
                metric="sentiment"
            )
            time_series_response = self.analytics_client.GetTimeSeriesData(time_series_request)
            
            # Get top keywords
            keywords_request = analytics_pb2.TopKeywordsRequest(
                time_range="24h",
                limit=10
            )
            keywords_response = self.analytics_client.GetTopKeywords(keywords_request)
            
            # Get real-time stats
            real_time_request = analytics_pb2.RealTimeStatsRequest(metric="all")
            real_time_response = self.analytics_client.GetRealTimeStats(real_time_request)
            
            analytics_data = {
                'sentiment_distribution': {
                    'sentiment_counts': dict(sentiment_response.sentiment_counts),
                    'total_comments': sentiment_response.total_comments,
                    'positive_percentage': sentiment_response.positive_percentage,
                    'negative_percentage': sentiment_response.negative_percentage,
                    'neutral_percentage': sentiment_response.neutral_percentage
                },
                'category_distribution': {
                    'category_counts': dict(category_response.category_counts),
                    'total_comments': category_response.total_comments,
                    'category_percentages': [
                        {
                            'category': cp.category,
                            'count': cp.count,
                            'percentage': cp.percentage
                        } for cp in category_response.category_percentages
                    ]
                },
                'time_series': {
                    'data_points': [
                        {
                            'timestamp': dp.timestamp,
                            'values': dict(dp.values),
                            'total_count': dp.total_count
                        } for dp in time_series_response.data_points
                    ],
                    'time_range': time_series_response.time_range,
                    'interval': time_series_response.interval,
                    'metric': time_series_response.metric
                },
                'keywords': {
                    'keywords': [
                        {
                            'keyword': kw.keyword,
                            'frequency': kw.frequency,
                            'percentage': kw.percentage
                        } for kw in keywords_response.keywords
                    ],
                    'total_comments': keywords_response.total_comments,
                    'time_range': keywords_response.time_range
                },
                'real_time_stats': {
                    'comments_per_minute': real_time_response.comments_per_minute,
                    'positive_ratio': real_time_response.positive_ratio,
                    'negative_ratio': real_time_response.negative_ratio,
                    'neutral_ratio': real_time_response.neutral_ratio,
                    'category_breakdown': dict(real_time_response.category_breakdown),
                    'active_consumers': real_time_response.active_consumers,
                    'queue_size': real_time_response.queue_size,
                    'timestamp': real_time_response.timestamp
                }
            }
            
            return analytics_data
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error getting analytics data: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error getting analytics data: {e}")
            return {}
    
    def save_analytics_to_database(self, analytics_data: Dict[str, Any]):
        """Save analytics data to database"""
        try:
            if not self._ensure_db_connection():
                logger.warning("Database connection not available for saving analytics")
                return
            
            cursor = self.db_conn.cursor()
            
            # Save sentiment distribution
            sentiment_data = analytics_data.get('sentiment_distribution', {})
            cursor.execute("""
                INSERT INTO analytics_sentiment_distribution 
                (timestamp, positive_count, negative_count, neutral_count, total_comments,
                 positive_percentage, negative_percentage, neutral_percentage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(),
                sentiment_data.get('sentiment_counts', {}).get('positive', 0),
                sentiment_data.get('sentiment_counts', {}).get('negative', 0),
                sentiment_data.get('sentiment_counts', {}).get('neutral', 0),
                sentiment_data.get('total_comments', 0),
                sentiment_data.get('positive_percentage', 0),
                sentiment_data.get('negative_percentage', 0),
                sentiment_data.get('neutral_percentage', 0)
            ))
            
            # Save category distribution
            category_data = analytics_data.get('category_distribution', {})
            for cp in category_data.get('category_percentages', []):
                cursor.execute("""
                    INSERT INTO analytics_category_distribution 
                    (timestamp, category, count, percentage)
                    VALUES (%s, %s, %s, %s)
                """, (
                    datetime.now(),
                    cp['category'],
                    cp['count'],
                    cp['percentage']
                ))
            
            # Save time series data (trend analytics)
            time_series_data = analytics_data.get('time_series', {})
            for dp in time_series_data.get('data_points', []):
                cursor.execute("""
                    INSERT INTO analytics_time_series 
                    (timestamp, time_bucket, metric, values, total_count)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    datetime.now(),
                    dp['timestamp'],
                    time_series_data.get('metric', 'sentiment'),
                    json.dumps(dp['values']),
                    dp['total_count']
                ))
            
            # Save keyword analytics
            keywords_data = analytics_data.get('keywords', {})
            for kw in keywords_data.get('keywords', []):
                cursor.execute("""
                    INSERT INTO analytics_keywords 
                    (timestamp, keyword, frequency, percentage, total_comments)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    datetime.now(),
                    kw['keyword'],
                    kw['frequency'],
                    kw['percentage'],
                    keywords_data.get('total_comments', 0)
                ))
            
            # Save real-time stats
            real_time_data = analytics_data.get('real_time_stats', {})
            cursor.execute("""
                INSERT INTO analytics_real_time_stats 
                (timestamp, comments_per_minute, positive_ratio, negative_ratio, neutral_ratio,
                 active_consumers, queue_size)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(),
                real_time_data.get('comments_per_minute', 0),
                real_time_data.get('positive_ratio', 0),
                real_time_data.get('negative_ratio', 0),
                real_time_data.get('neutral_ratio', 0),
                real_time_data.get('active_consumers', 0),
                real_time_data.get('queue_size', 0)
            ))
            
            self.db_conn.commit()
            logger.info("Analytics data saved to database")
            
        except Exception as e:
            logger.error(f"Error saving analytics to database: {e}")
            if self.db_conn:
                self.db_conn.rollback()
    
    def publish_analytics_results(self, analytics_data: Dict[str, Any]):
        """Publish analytics results to different Kafka topics"""
        try:
            timestamp = datetime.now().isoformat()
            
            # Publish sentiment analytics
            if 'sentiment_distribution' in analytics_data:
                sentiment_message = {
                    'timestamp': timestamp,
                    'type': 'sentiment_analytics',
                    'data': analytics_data['sentiment_distribution']
                }
                self.producer.send('sentiment-analytics', sentiment_message)
                logger.info("Sentiment analytics published to sentiment-analytics topic")
            
            # Publish category analytics
            if 'category_distribution' in analytics_data:
                category_message = {
                    'timestamp': timestamp,
                    'type': 'category_analytics',
                    'data': analytics_data['category_distribution']
                }
                self.producer.send('category-analytics', category_message)
                logger.info("Category analytics published to category-analytics topic")
            
            # Publish time series analytics
            if 'time_series' in analytics_data:
                time_series_message = {
                    'timestamp': timestamp,
                    'type': 'trend_analytics',
                    'data': analytics_data['time_series']
                }
                self.producer.send('trend-analytics', time_series_message)
                logger.info("Trend analytics published to trend-analytics topic")
            
            # Publish keyword analytics
            if 'keywords' in analytics_data:
                keyword_message = {
                    'timestamp': timestamp,
                    'type': 'keyword_analytics',
                    'data': analytics_data['keywords']
                }
                self.producer.send('keyword-analytics', keyword_message)
                logger.info("Keyword analytics published to keyword-analytics topic")
            
            # Publish real-time stats
            if 'real_time_stats' in analytics_data:
                real_time_message = {
                    'timestamp': timestamp,
                    'type': 'real_time_analytics',
                    'data': analytics_data['real_time_stats']
                }
                self.producer.send('real-time-analytics', real_time_message)
                logger.info("Real-time analytics published to real-time-analytics topic")
            
            # Publish combined analytics (for backward compatibility)
            combined_message = {
                'timestamp': timestamp,
                'type': 'combined_analytics',
                'analytics_data': analytics_data
            }
            self.producer.send('analytics-results', combined_message)
            
            self.producer.flush()
            logger.info("All analytics results published to Kafka topics")
            
        except Exception as e:
            logger.error(f"Error publishing analytics results: {e}")
    
    def process_comment(self, comment_data: Dict[str, Any]):
        """Process a single comment"""
        try:
            logger.info(f"Processing comment: {comment_data.get('commentId')}")
            
            # Update real-time metrics
            self.update_real_time_metrics(comment_data)
            
            # Get analytics data from gRPC service
            analytics_data = self.get_analytics_data(comment_data)
            
            if analytics_data:
                # Save to database
                self.save_analytics_to_database(analytics_data)
                
                # Publish to Kafka
                self.publish_analytics_results(analytics_data)
                
                logger.info(f"Successfully processed analytics for comment: {comment_data.get('commentId')}")
            else:
                logger.warning(f"No analytics data available for comment: {comment_data.get('commentId')}")
                
        except Exception as e:
            logger.error(f"Error processing comment: {e}")
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting analytics consumer...")
        
        try:
            for message in self.consumer:
                try:
                    comment_data = message.value
                    self.process_comment(comment_data)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Shutting down analytics consumer...")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()
            self.producer.close()
            if self.db_conn:
                self.db_conn.close()

if __name__ == '__main__':
    consumer = AnalyticsConsumer()
    consumer.run() 