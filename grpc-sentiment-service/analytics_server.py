import grpc
import time
import logging
import psycopg2
import redis
from datetime import datetime, timedelta
from concurrent import futures
import json
from typing import Dict, List, Optional

# Import generated protobuf
import analytics_pb2
import analytics_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsService(analytics_pb2_grpc.AnalyticsServiceServicer):
    def __init__(self):
        self.db_config = {
            'host': 'postgres',
            'database': 'chatbot_db',
            'user': 'chatbot_user',
            'password': 'chatbot_pass'
        }
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        self._init_db_connection()
    
    def _init_db_connection(self):
        """Initialize database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            # Set autocommit to True to avoid transaction issues
            self.conn.autocommit = True
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self.conn = None
    
    def _ensure_db_connection(self):
        """Ensure database connection is available, reconnect if needed"""
        if self.conn is None:
            logger.info("Attempting to reconnect to database...")
            self._init_db_connection()
        else:
            try:
                # Test connection
                cursor = self.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
            except Exception as e:
                logger.warning(f"Database connection test failed: {e}")
                logger.info("Reconnecting to database...")
                try:
                    self.conn.close()
                except:
                    pass
                self.conn = None
                self._init_db_connection()
        return self.conn is not None
    
    def _execute_query_safely(self, query, params=None):
        """Execute query with proper error handling"""
        try:
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                return None
            
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            return results
            
        except Exception as e:
            logger.error(f"Database query error: {e}")
            # Reset connection on error
            try:
                if self.conn:
                    self.conn.close()
            except:
                pass
            self.conn = None
            return None
    
    def _get_time_range_filter(self, time_range: str) -> str:
        """Convert time range to SQL filter"""
        now = datetime.now()
        if time_range == "1h":
            start_time = now - timedelta(hours=1)
        elif time_range == "24h":
            start_time = now - timedelta(days=1)
        elif time_range == "7d":
            start_time = now - timedelta(days=7)
        elif time_range == "30d":
            start_time = now - timedelta(days=30)
        else:
            start_time = now - timedelta(hours=1)  # default to 1 hour
        
        return start_time.strftime('%Y-%m-%d %H:%M:%S')
    
    def GetAllAnalytics(self, request, context):
        """Get all analytics data in a single response"""
        try:
            logger.info(f"GetAllAnalytics called with time_range: {request.time_range}")
            
            # Get sentiment distribution
            sentiment_response = self._get_sentiment_distribution_internal(
                request.time_range, request.category_filter
            )
            
            # Get category distribution
            category_response = self._get_category_distribution_internal(
                request.time_range, request.sentiment_filter
            )
            
            # Get time series data
            time_series_response = self._get_time_series_data_internal(
                request.time_range, request.interval or "1h", "sentiment"
            )
            
            # Get top keywords
            keywords_response = self._get_top_keywords_internal(
                request.time_range, request.sentiment_filter, 
                request.category_filter, request.keyword_limit or 10
            )
            
            # Get real-time stats
            real_time_response = self._get_real_time_stats_internal()
            
            # Return combined response
            return analytics_pb2.AnalyticsResponse(
                sentiment=sentiment_response,
                category=category_response,
                time_series=time_series_response,
                keywords=keywords_response,
                real_time=real_time_response
            )
            
        except Exception as e:
            logger.error(f"Error in GetAllAnalytics: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.AnalyticsResponse()
    
    def _get_sentiment_distribution_internal(self, time_range: str, category_filter: str = "") -> analytics_pb2.SentimentDistributionResponse:
        """Internal method to get sentiment distribution"""
        try:
            time_filter = self._get_time_range_filter(time_range)
            category_filter_sql = f"AND category = '{category_filter}'" if category_filter else ""
            
            query = f"""
                SELECT sentiment, COUNT(*) as count
                FROM processed_comments 
                WHERE timestamp >= '{time_filter}' {category_filter_sql}
                GROUP BY sentiment
            """
            
            results = self._execute_query_safely(query)
            if results is None:
                return analytics_pb2.SentimentDistributionResponse()
            
            sentiment_counts = {}
            total_comments = 0
            
            for sentiment, count in results:
                sentiment_counts[sentiment] = count
                total_comments += count
            
            # Calculate percentages
            positive_percentage = (sentiment_counts.get('positive', 0) / total_comments * 100) if total_comments > 0 else 0
            negative_percentage = (sentiment_counts.get('negative', 0) / total_comments * 100) if total_comments > 0 else 0
            neutral_percentage = (sentiment_counts.get('neutral', 0) / total_comments * 100) if total_comments > 0 else 0
            
            return analytics_pb2.SentimentDistributionResponse(
                sentiment_counts=sentiment_counts,
                total_comments=total_comments,
                positive_percentage=positive_percentage,
                negative_percentage=negative_percentage,
                neutral_percentage=neutral_percentage,
                time_range=time_range
            )
            
        except Exception as e:
            logger.error(f"Error in _get_sentiment_distribution_internal: {e}")
            return analytics_pb2.SentimentDistributionResponse()
    
    def _get_category_distribution_internal(self, time_range: str, sentiment_filter: str = "") -> analytics_pb2.CategoryDistributionResponse:
        """Internal method to get category distribution"""
        try:
            # Check if database connection is available
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                return analytics_pb2.CategoryDistributionResponse()
            
            time_filter = self._get_time_range_filter(time_range)
            sentiment_filter_sql = f"AND sentiment = '{sentiment_filter}'" if sentiment_filter else ""
            
            query = f"""
                SELECT category, COUNT(*) as count
                FROM processed_comments 
                WHERE timestamp >= '{time_filter}' {sentiment_filter_sql}
                GROUP BY category
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            category_counts = {}
            total_comments = 0
            category_percentages = []
            
            for category, count in results:
                category_counts[category] = count
                total_comments += count
            
            # Calculate percentages
            for category, count in results:
                percentage = (count / total_comments * 100) if total_comments > 0 else 0
                category_percentages.append(analytics_pb2.CategoryPercentage(
                    category=category,
                    count=count,
                    percentage=percentage
                ))
            
            return analytics_pb2.CategoryDistributionResponse(
                category_counts=category_counts,
                total_comments=total_comments,
                category_percentages=category_percentages,
                time_range=time_range
            )
            
        except Exception as e:
            logger.error(f"Error in _get_category_distribution_internal: {e}")
            return analytics_pb2.CategoryDistributionResponse()
    
    def _get_time_series_data_internal(self, time_range: str, interval: str, metric: str) -> analytics_pb2.TimeSeriesResponse:
        """Internal method to get time series data"""
        try:
            # Check if database connection is available
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                return analytics_pb2.TimeSeriesResponse()
            
            time_filter = self._get_time_range_filter(time_range)
            
            # Determine interval format based on request
            if interval == "1m":
                group_by = "DATE_TRUNC('minute', timestamp)"
            elif interval == "5m":
                group_by = "DATE_TRUNC('minute', timestamp) - INTERVAL '5 minutes' * (EXTRACT(MINUTE FROM timestamp)::int / 5)"
            elif interval == "1h":
                group_by = "DATE_TRUNC('hour', timestamp)"
            elif interval == "1d":
                group_by = "DATE_TRUNC('day', timestamp)"
            else:
                group_by = "DATE_TRUNC('minute', timestamp)"
            
            if metric == "sentiment":
                query = f"""
                    SELECT {group_by} as time_bucket, sentiment, COUNT(*) as count
                    FROM processed_comments 
                    WHERE timestamp >= '{time_filter}'
                    GROUP BY {group_by}, sentiment
                    ORDER BY time_bucket
                """
            elif metric == "category":
                query = f"""
                    SELECT {group_by} as time_bucket, category, COUNT(*) as count
                    FROM processed_comments 
                    WHERE timestamp >= '{time_filter}'
                    GROUP BY {group_by}, category
                    ORDER BY time_bucket
                """
            else:  # volume
                query = f"""
                    SELECT {group_by} as time_bucket, COUNT(*) as count
                    FROM processed_comments 
                    WHERE timestamp >= '{time_filter}'
                    GROUP BY {group_by}
                    ORDER BY time_bucket
                """
            
            logger.info(f"Trend SQL: {query}")
            results = self._execute_query_safely(query)
            logger.info(f"Trend SQL result count: {len(results) if results else 0}")
            
            # Group by timestamp
            time_series_data = {}
            for row in results or []:
                if metric == "volume":
                    timestamp, count = row
                    if timestamp not in time_series_data:
                        time_series_data[timestamp] = {"total": 0}
                    time_series_data[timestamp]["total"] = count
                else:
                    timestamp, metric_value, count = row
                    if timestamp not in time_series_data:
                        time_series_data[timestamp] = {}
                    time_series_data[timestamp][metric_value] = count
            
            data_points = []
            for timestamp, values in time_series_data.items():
                total_count = values.get("total", sum(values.values()))
                data_points.append(analytics_pb2.TimeSeriesPoint(
                    timestamp=timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    values=values,
                    total_count=total_count
                ))
            
            return analytics_pb2.TimeSeriesResponse(
                data_points=data_points,
                time_range=time_range,
                interval=interval,
                metric=metric
            )
            
        except Exception as e:
            logger.error(f"Error in _get_time_series_data_internal: {e}")
            return analytics_pb2.TimeSeriesResponse()
    
    def _get_top_keywords_internal(self, time_range: str, sentiment_filter: str = "", 
                                  category_filter: str = "", limit: int = 10) -> analytics_pb2.TopKeywordsResponse:
        """Internal method to get top keywords"""
        try:
            # Check if database connection is available
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                return analytics_pb2.TopKeywordsResponse()
            
            time_filter = self._get_time_range_filter(time_range)
            sentiment_filter_sql = f"AND sentiment = '{sentiment_filter}'" if sentiment_filter else ""
            category_filter_sql = f"AND category = '{category_filter}'" if category_filter else ""
            
            # Use subquery for keyword extraction to avoid set-returning function in WHERE
            query = f"""
                SELECT 
                    keyword,
                    COUNT(*) as frequency
                FROM (
                    SELECT unnest(string_to_array(lower(message), ' ')) as keyword
                    FROM processed_comments
                    WHERE timestamp >= '{time_filter}' {sentiment_filter_sql} {category_filter_sql}
                ) AS words
                WHERE length(keyword) > 3
                GROUP BY keyword
                HAVING COUNT(*) > 1
                ORDER BY frequency DESC
                LIMIT {limit}
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Get total comments for percentage calculation
            total_query = f"""
                SELECT COUNT(*) FROM processed_comments 
                WHERE timestamp >= '{time_filter}' {sentiment_filter_sql} {category_filter_sql}
            """
            cursor.execute(total_query)
            total_comments = cursor.fetchone()[0]
            
            keywords = []
            for keyword, frequency in results:
                percentage = (frequency / total_comments * 100) if total_comments > 0 else 0
                keywords.append(analytics_pb2.KeywordFrequency(
                    keyword=keyword,
                    frequency=frequency,
                    percentage=percentage
                ))
            
            return analytics_pb2.TopKeywordsResponse(
                keywords=keywords,
                total_comments=total_comments,
                time_range=time_range
            )
            
        except Exception as e:
            logger.error(f"Error in _get_top_keywords_internal: {e}")
            return analytics_pb2.TopKeywordsResponse()
    
    def _get_real_time_stats_internal(self) -> analytics_pb2.RealTimeStatsResponse:
        """Internal method to get real-time statistics"""
        try:
            # Get real-time metrics from Redis
            comments_per_minute = float(self.redis_client.get('comments_per_minute') or 0)
            positive_ratio = float(self.redis_client.get('positive_ratio') or 0)
            negative_ratio = float(self.redis_client.get('negative_ratio') or 0)
            neutral_ratio = float(self.redis_client.get('neutral_ratio') or 0)
            active_consumers = int(self.redis_client.get('active_consumers') or 0)
            queue_size = int(self.redis_client.get('queue_size') or 0)
            
            # Get category breakdown
            category_breakdown = {}
            categories = ['menü', 'lezzet', 'servis', 'ambiyans', 'fiyat']
            for category in categories:
                count = int(self.redis_client.get(f'category_{category}') or 0)
                category_breakdown[category] = count
            
            return analytics_pb2.RealTimeStatsResponse(
                comments_per_minute=comments_per_minute,
                positive_ratio=positive_ratio,
                negative_ratio=negative_ratio,
                neutral_ratio=neutral_ratio,
                category_breakdown=category_breakdown,
                active_consumers=active_consumers,
                queue_size=queue_size,
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error in _get_real_time_stats_internal: {e}")
            return analytics_pb2.RealTimeStatsResponse()
    
    def GetSentimentDistribution(self, request, context):
        """Get sentiment distribution for given time range"""
        try:
            # Check if database connection is available
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Database connection not available")
                return analytics_pb2.SentimentDistributionResponse()
            
            time_filter = self._get_time_range_filter(request.time_range)
            category_filter = f"AND category = '{request.category_filter}'" if request.category_filter else ""
            
            query = f"""
                SELECT sentiment, COUNT(*) as count
                FROM processed_comments 
                WHERE timestamp >= '{time_filter}' {category_filter}
                GROUP BY sentiment
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            sentiment_counts = {}
            total_comments = 0
            
            for sentiment, count in results:
                sentiment_counts[sentiment] = count
                total_comments += count
            
            # Calculate percentages
            positive_percentage = (sentiment_counts.get('positive', 0) / total_comments * 100) if total_comments > 0 else 0
            negative_percentage = (sentiment_counts.get('negative', 0) / total_comments * 100) if total_comments > 0 else 0
            neutral_percentage = (sentiment_counts.get('neutral', 0) / total_comments * 100) if total_comments > 0 else 0
            
            return analytics_pb2.SentimentDistributionResponse(
                sentiment_counts=sentiment_counts,
                total_comments=total_comments,
                positive_percentage=positive_percentage,
                negative_percentage=negative_percentage,
                neutral_percentage=neutral_percentage,
                time_range=request.time_range
            )
            
        except Exception as e:
            logger.error(f"Error in GetSentimentDistribution: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.SentimentDistributionResponse()
    
    def GetCategoryDistribution(self, request, context):
        """Get category distribution for given time range"""
        try:
            # Check if database connection is available
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Database connection not available")
                return analytics_pb2.CategoryDistributionResponse()
            
            time_filter = self._get_time_range_filter(request.time_range)
            sentiment_filter = f"AND sentiment = '{request.sentiment_filter}'" if request.sentiment_filter else ""
            
            query = f"""
                SELECT category, COUNT(*) as count
                FROM processed_comments 
                WHERE timestamp >= '{time_filter}' {sentiment_filter}
                GROUP BY category
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            category_counts = {}
            total_comments = 0
            category_percentages = []
            
            for category, count in results:
                category_counts[category] = count
                total_comments += count
            
            # Calculate percentages
            for category, count in results:
                percentage = (count / total_comments * 100) if total_comments > 0 else 0
                category_percentages.append(analytics_pb2.CategoryPercentage(
                    category=category,
                    count=count,
                    percentage=percentage
                ))
            
            return analytics_pb2.CategoryDistributionResponse(
                category_counts=category_counts,
                total_comments=total_comments,
                category_percentages=category_percentages,
                time_range=request.time_range
            )
            
        except Exception as e:
            logger.error(f"Error in GetCategoryDistribution: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.CategoryDistributionResponse()
    
    def GetTimeSeriesData(self, request, context):
        """Get time series data for given metric"""
        try:
            time_filter = self._get_time_range_filter(request.time_range)
            
            # Determine interval format based on request
            if request.interval == "1m":
                interval_format = "YYYY-MM-DD HH24:MI"
                group_by = "DATE_TRUNC('minute', timestamp)"
            elif request.interval == "5m":
                interval_format = "YYYY-MM-DD HH24:MI"
                group_by = "DATE_TRUNC('minute', timestamp) - INTERVAL '5 minutes' * (EXTRACT(MINUTE FROM timestamp)::int / 5)"
            elif request.interval == "1h":
                interval_format = "YYYY-MM-DD HH24"
                group_by = "DATE_TRUNC('hour', timestamp)"
            elif request.interval == "1d":
                interval_format = "YYYY-MM-DD"
                group_by = "DATE_TRUNC('day', timestamp)"
            else:
                interval_format = "YYYY-MM-DD HH24:MI"
                group_by = "DATE_TRUNC('minute', timestamp)"
            
            if request.metric == "sentiment":
                query = f"""
                    SELECT {group_by} as time_bucket, sentiment, COUNT(*) as count
                    FROM processed_comments 
                    WHERE timestamp >= '{time_filter}'
                    GROUP BY {group_by}, sentiment
                    ORDER BY time_bucket
                """
            elif request.metric == "category":
                query = f"""
                    SELECT {group_by} as time_bucket, category, COUNT(*) as count
                    FROM processed_comments 
                    WHERE timestamp >= '{time_filter}'
                    GROUP BY {group_by}, category
                    ORDER BY time_bucket
                """
            else:  # volume
                query = f"""
                    SELECT {group_by} as time_bucket, COUNT(*) as count
                    FROM processed_comments 
                    WHERE timestamp >= '{time_filter}'
                    GROUP BY {group_by}
                    ORDER BY time_bucket
                """
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Group by timestamp
            time_series_data = {}
            for row in results:
                if request.metric == "volume":
                    timestamp, count = row
                    if timestamp not in time_series_data:
                        time_series_data[timestamp] = {"total": 0}
                    time_series_data[timestamp]["total"] = count
                else:
                    timestamp, metric_value, count = row
                    if timestamp not in time_series_data:
                        time_series_data[timestamp] = {}
                    time_series_data[timestamp][metric_value] = count
            
            data_points = []
            for timestamp, values in time_series_data.items():
                total_count = values.get("total", sum(values.values()))
                data_points.append(analytics_pb2.TimeSeriesPoint(
                    timestamp=timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    values=values,
                    total_count=total_count
                ))
            
            return analytics_pb2.TimeSeriesResponse(
                data_points=data_points,
                time_range=request.time_range,
                interval=request.interval,
                metric=request.metric
            )
            
        except Exception as e:
            logger.error(f"Error in GetTimeSeriesData: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.TimeSeriesResponse()
    
    def GetTopKeywords(self, request, context):
        """Get top keywords from comments"""
        try:
            # Check if database connection is available
            if not self._ensure_db_connection():
                logger.error("Database connection is not available")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Database connection not available")
                return analytics_pb2.TopKeywordsResponse()
            
            # Use the internal method that has the correct SQL query
            return self._get_top_keywords_internal(
                time_range=request.time_range,
                sentiment_filter=request.sentiment_filter,
                category_filter=request.category_filter,
                limit=request.limit or 10
            )
            
        except Exception as e:
            logger.error(f"Error in GetTopKeywords: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.TopKeywordsResponse()
    
    def GetServiceMetrics(self, request, context):
        """Get service performance metrics"""
        try:
            time_filter = self._get_time_range_filter(request.time_range)
            
            # Get metrics from Redis cache
            total_requests = int(self.redis_client.get('total_requests') or 0)
            successful_requests = int(self.redis_client.get('successful_requests') or 0)
            failed_requests = int(self.redis_client.get('failed_requests') or 0)
            
            # Calculate success rate
            success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
            
            # Get average response time
            avg_response_time = float(self.redis_client.get('avg_response_time') or 0)
            
            # Get requests per second
            requests_per_second = float(self.redis_client.get('requests_per_second') or 0)
            
            # Health checks
            health_checks = []
            services = ['sentiment-service', 'analytics-service', 'kafka-producer', 'kafka-consumer']
            
            for service in services:
                status = self.redis_client.get(f'{service}_status') or 'unknown'
                response_time = float(self.redis_client.get(f'{service}_response_time') or 0)
                last_check = self.redis_client.get(f'{service}_last_check') or 'unknown'
                
                health_checks.append(analytics_pb2.ServiceHealthCheck(
                    service_name=service,
                    status=status,
                    response_time=response_time,
                    last_check=last_check
                ))
            
            return analytics_pb2.ServiceMetricsResponse(
                average_response_time=avg_response_time,
                total_requests=total_requests,
                successful_requests=successful_requests,
                failed_requests=failed_requests,
                success_rate=success_rate,
                requests_per_second=requests_per_second,
                health_checks=health_checks
            )
            
        except Exception as e:
            logger.error(f"Error in GetServiceMetrics: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.ServiceMetricsResponse()
    
    def GetRealTimeStats(self, request, context):
        """Get real-time statistics"""
        try:
            # Get real-time metrics from Redis
            comments_per_minute = float(self.redis_client.get('comments_per_minute') or 0)
            positive_ratio = float(self.redis_client.get('positive_ratio') or 0)
            negative_ratio = float(self.redis_client.get('negative_ratio') or 0)
            neutral_ratio = float(self.redis_client.get('neutral_ratio') or 0)
            active_consumers = int(self.redis_client.get('active_consumers') or 0)
            queue_size = int(self.redis_client.get('queue_size') or 0)
            
            # Get category breakdown
            category_breakdown = {}
            categories = ['menü', 'lezzet', 'servis', 'ambiyans', 'fiyat']
            for category in categories:
                count = int(self.redis_client.get(f'category_{category}') or 0)
                category_breakdown[category] = count
            
            return analytics_pb2.RealTimeStatsResponse(
                comments_per_minute=comments_per_minute,
                positive_ratio=positive_ratio,
                negative_ratio=negative_ratio,
                neutral_ratio=neutral_ratio,
                category_breakdown=category_breakdown,
                active_consumers=active_consumers,
                queue_size=queue_size,
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error in GetRealTimeStats: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return analytics_pb2.RealTimeStatsResponse()

def serve():
    """Start the analytics gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analytics_pb2_grpc.add_AnalyticsServiceServicer_to_server(AnalyticsService(), server)
    
    # Listen on port 50052 (different from sentiment service)
    listen_addr = '[::]:50052'
    server.add_insecure_port(listen_addr)
    
    logger.info(f"Analytics gRPC server starting on {listen_addr}")
    server.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down analytics server...")
        server.stop(0)

if __name__ == '__main__':
    serve() 