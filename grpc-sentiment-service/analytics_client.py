import grpc
import logging
import time
from typing import Dict, List, Optional

# Import generated protobuf
import analytics_pb2
import analytics_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsClient:
    def __init__(self, host: str = 'localhost', port: int = 50052):
        """Initialize analytics gRPC client"""
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = analytics_pb2_grpc.AnalyticsServiceStub(self.channel)
        logger.info(f"Analytics client initialized for {host}:{port}")
    
    def get_all_analytics(self, time_range: str = "24h", interval: str = "1h", 
                         category_filter: str = "", sentiment_filter: str = "", 
                         keyword_limit: int = 10) -> Dict:
        """Get all analytics data in a single request"""
        try:
            request = analytics_pb2.AnalyticsRequest(
                time_range=time_range,
                interval=interval,
                category_filter=category_filter,
                sentiment_filter=sentiment_filter,
                keyword_limit=keyword_limit
            )
            
            response = self.stub.GetAllAnalytics(request)
            
            # Convert response to dictionary
            analytics_data = {
                'sentiment_distribution': {
                    'sentiment_counts': dict(response.sentiment.sentiment_counts),
                    'total_comments': response.sentiment.total_comments,
                    'positive_percentage': response.sentiment.positive_percentage,
                    'negative_percentage': response.sentiment.negative_percentage,
                    'neutral_percentage': response.sentiment.neutral_percentage,
                    'time_range': response.sentiment.time_range
                },
                'category_distribution': {
                    'category_counts': dict(response.category.category_counts),
                    'total_comments': response.category.total_comments,
                    'category_percentages': [
                        {
                            'category': cp.category,
                            'count': cp.count,
                            'percentage': cp.percentage
                        } for cp in response.category.category_percentages
                    ],
                    'time_range': response.category.time_range
                },
                'time_series': {
                    'data_points': [
                        {
                            'timestamp': dp.timestamp,
                            'values': dict(dp.values),
                            'total_count': dp.total_count
                        } for dp in response.time_series.data_points
                    ],
                    'time_range': response.time_series.time_range,
                    'interval': response.time_series.interval,
                    'metric': response.time_series.metric
                },
                'keywords': {
                    'keywords': [
                        {
                            'keyword': kw.keyword,
                            'frequency': kw.frequency,
                            'percentage': kw.percentage
                        } for kw in response.keywords.keywords
                    ],
                    'total_comments': response.keywords.total_comments,
                    'time_range': response.keywords.time_range
                },
                'real_time_stats': {
                    'comments_per_minute': response.real_time.comments_per_minute,
                    'positive_ratio': response.real_time.positive_ratio,
                    'negative_ratio': response.real_time.negative_ratio,
                    'neutral_ratio': response.real_time.neutral_ratio,
                    'category_breakdown': dict(response.real_time.category_breakdown),
                    'active_consumers': response.real_time.active_consumers,
                    'queue_size': response.real_time.queue_size,
                    'timestamp': response.real_time.timestamp
                }
            }
            
            return analytics_data
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_all_analytics: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_all_analytics: {e}")
            return {}
    
    def get_sentiment_distribution(self, time_range: str = "24h", category_filter: str = "") -> Dict:
        """Get sentiment distribution"""
        try:
            request = analytics_pb2.SentimentDistributionRequest(
                time_range=time_range,
                category_filter=category_filter
            )
            
            response = self.stub.GetSentimentDistribution(request)
            
            return {
                'sentiment_counts': dict(response.sentiment_counts),
                'total_comments': response.total_comments,
                'positive_percentage': response.positive_percentage,
                'negative_percentage': response.negative_percentage,
                'neutral_percentage': response.neutral_percentage,
                'time_range': response.time_range
            }
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_sentiment_distribution: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_sentiment_distribution: {e}")
            return {}
    
    def get_category_distribution(self, time_range: str = "24h", sentiment_filter: str = "") -> Dict:
        """Get category distribution"""
        try:
            request = analytics_pb2.CategoryDistributionRequest(
                time_range=time_range,
                sentiment_filter=sentiment_filter
            )
            
            response = self.stub.GetCategoryDistribution(request)
            
            category_percentages = []
            for cp in response.category_percentages:
                category_percentages.append({
                    'category': cp.category,
                    'count': cp.count,
                    'percentage': cp.percentage
                })
            
            return {
                'category_counts': dict(response.category_counts),
                'total_comments': response.total_comments,
                'category_percentages': category_percentages,
                'time_range': response.time_range
            }
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_category_distribution: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_category_distribution: {e}")
            return {}
    
    def get_time_series_data(self, time_range: str = "24h", interval: str = "1h", metric: str = "sentiment") -> Dict:
        """Get time series data"""
        try:
            request = analytics_pb2.TimeSeriesRequest(
                time_range=time_range,
                interval=interval,
                metric=metric
            )
            
            response = self.stub.GetTimeSeriesData(request)
            
            data_points = []
            for dp in response.data_points:
                data_points.append({
                    'timestamp': dp.timestamp,
                    'values': dict(dp.values),
                    'total_count': dp.total_count
                })
            
            return {
                'data_points': data_points,
                'time_range': response.time_range,
                'interval': response.interval,
                'metric': response.metric
            }
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_time_series_data: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_time_series_data: {e}")
            return {}
    
    def get_top_keywords(self, time_range: str = "24h", sentiment_filter: str = "", 
                        category_filter: str = "", limit: int = 10) -> Dict:
        """Get top keywords"""
        try:
            request = analytics_pb2.TopKeywordsRequest(
                time_range=time_range,
                sentiment_filter=sentiment_filter,
                category_filter=category_filter,
                limit=limit
            )
            
            response = self.stub.GetTopKeywords(request)
            
            keywords = []
            for kw in response.keywords:
                keywords.append({
                    'keyword': kw.keyword,
                    'frequency': kw.frequency,
                    'percentage': kw.percentage
                })
            
            return {
                'keywords': keywords,
                'total_comments': response.total_comments,
                'time_range': response.time_range
            }
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_top_keywords: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_top_keywords: {e}")
            return {}
    
    def get_service_metrics(self, service_name: str = "all", time_range: str = "24h") -> Dict:
        """Get service performance metrics"""
        try:
            request = analytics_pb2.ServiceMetricsRequest(
                service_name=service_name,
                time_range=time_range
            )
            
            response = self.stub.GetServiceMetrics(request)
            
            health_checks = []
            for hc in response.health_checks:
                health_checks.append({
                    'service_name': hc.service_name,
                    'status': hc.status,
                    'response_time': hc.response_time,
                    'last_check': hc.last_check
                })
            
            return {
                'average_response_time': response.average_response_time,
                'total_requests': response.total_requests,
                'successful_requests': response.successful_requests,
                'failed_requests': response.failed_requests,
                'success_rate': response.success_rate,
                'requests_per_second': response.requests_per_second,
                'health_checks': health_checks
            }
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_service_metrics: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_service_metrics: {e}")
            return {}
    
    def get_real_time_stats(self, metric: str = "all") -> Dict:
        """Get real-time statistics"""
        try:
            request = analytics_pb2.RealTimeStatsRequest(metric=metric)
            
            response = self.stub.GetRealTimeStats(request)
            
            return {
                'comments_per_minute': response.comments_per_minute,
                'positive_ratio': response.positive_ratio,
                'negative_ratio': response.negative_ratio,
                'neutral_ratio': response.neutral_ratio,
                'category_breakdown': dict(response.category_breakdown),
                'active_consumers': response.active_consumers,
                'queue_size': response.queue_size,
                'timestamp': response.timestamp
            }
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error in get_real_time_stats: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error in get_real_time_stats: {e}")
            return {}
    
    def close(self):
        """Close the gRPC channel"""
        self.channel.close()
        logger.info("Analytics client channel closed")

def test_analytics_client():
    """Test the analytics client"""
    client = AnalyticsClient()
    
    try:
        # Test getAllAnalytics
        print("Testing getAllAnalytics...")
        all_analytics = client.get_all_analytics("24h")
        print(f"All analytics data: {all_analytics}")
        
        # Test individual methods
        print("\nTesting individual methods...")
        sentiment_data = client.get_sentiment_distribution("24h")
        print(f"Sentiment distribution: {sentiment_data}")
        
        category_data = client.get_category_distribution("24h")
        print(f"Category distribution: {category_data}")
        
        real_time_data = client.get_real_time_stats()
        print(f"Real-time stats: {real_time_data}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    test_analytics_client() 