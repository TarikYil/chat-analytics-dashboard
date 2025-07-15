import grpc
import time
import logging
from concurrent.futures import ThreadPoolExecutor
import sentiment_pb2
import sentiment_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentAnalysisClient:
    def __init__(self, host='localhost', port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = sentiment_pb2_grpc.SentimentAnalysisStub(self.channel)
        self.max_retries = 3
        self.retry_delay = 1.0
    
    def analyze_sentiment(self, text: str, language: str = 'tr', request_id: str = None) -> dict:
        """Analyze sentiment with retry mechanism"""
        if not request_id:
            request_id = f"client_{int(time.time() * 1000)}"
        
        request = sentiment_pb2.SentimentRequest(
            text=text,
            language=language,
            request_id=request_id
        )
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting sentiment analysis (attempt {attempt + 1}): {request_id}")
                
                response = self.stub.AnalyzeSentiment(request)
                
                result = {
                    'sentiment': response.sentiment,
                    'confidence': response.confidence,
                    'processing_time': response.processing_time,
                    'request_id': response.request_id,
                    'status': response.status,
                    'error_message': response.error_message
                }
                
                if response.status == 'success':
                    logger.info(f"Successfully analyzed sentiment: {result}")
                    return result
                elif response.status == 'rate_limited':
                    logger.warning(f"Rate limited: {response.error_message}")
                    time.sleep(self.retry_delay * (attempt + 1))
                elif response.status == 'dropped':
                    logger.warning(f"Request dropped: {response.error_message}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Error: {response.error_message}")
                    time.sleep(self.retry_delay * (attempt + 1))
                    
            except grpc.RpcError as e:
                logger.error(f"gRPC error (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    return {
                        'sentiment': 'neutral',
                        'confidence': 0.0,
                        'processing_time': 0.0,
                        'request_id': request_id,
                        'status': 'error',
                        'error_message': f'gRPC error: {str(e)}'
                    }
        
        return {
            'sentiment': 'neutral',
            'confidence': 0.0,
            'processing_time': 0.0,
            'request_id': request_id,
            'status': 'max_retries_exceeded',
            'error_message': 'Max retries exceeded'
        }
    
    def get_service_status(self) -> dict:
        """Get service status"""
        try:
            request = sentiment_pb2.StatusRequest(service_name="sentiment_analysis")
            response = self.stub.GetServiceStatus(request)
            
            return {
                'status': response.status,
                'requests_per_second': response.requests_per_second,
                'total_requests': response.total_requests,
                'average_response_time': response.average_response_time,
                'rate_limit': response.rate_limit,
                'current_queue_size': response.current_queue_size
            }
        except grpc.RpcError as e:
            logger.error(f"Error getting service status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def close(self):
        """Close the gRPC channel"""
        self.channel.close()

def test_sentiment_analysis():
    """Test sentiment analysis with multiple requests"""
    client = SentimentAnalysisClient()
    
    test_texts = [
        "Yemekler çok lezzetliydi, tadı damağımda kaldı!",
        "Servis çok yavaştı, beklemek zorunda kaldık.",
        "Ortam normal, standart bir restoran atmosferi.",
        "Fiyatlar çok yüksek, kaliteye değmez.",
        "Personel çok hızlı ve verimli, teşekkürler."
    ]
    
    print("Testing sentiment analysis...")
    
    # Test individual requests
    for i, text in enumerate(test_texts):
        print(f"\nTest {i+1}: {text}")
        result = client.analyze_sentiment(text, request_id=f"test_{i+1}")
        print(f"Result: {result}")
        time.sleep(0.1)  # Small delay between requests
    
    # Test service status
    print("\nService Status:")
    status = client.get_service_status()
    print(f"Status: {status}")
    
    client.close()

if __name__ == '__main__':
    test_sentiment_analysis() 