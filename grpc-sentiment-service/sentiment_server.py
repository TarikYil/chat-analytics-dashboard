import grpc
import time
import random
import threading
from concurrent import futures
from collections import defaultdict, deque
import logging
from datetime import datetime, timedelta
import json
import redis

# Import generated protobuf
import sentiment_pb2
import sentiment_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SentimentAnalysisServicer(sentiment_pb2_grpc.SentimentAnalysisServicer):
    def __init__(self):
        self.rate_limit = 100  # requests per second
        self.request_times = deque()
        self.total_requests = 0
        self.cache = {}  # Simple in-memory cache
        self.lock = threading.Lock()
        
        # Redis connection for distributed caching
        try:
            self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
            logger.info("Connected to Redis for caching")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            self.redis_client = None
        
        # Statistics
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'rate_limited_requests': 0,
            'dropped_requests': 0,
            'cached_requests': 0,
            'average_response_time': 0.0
        }
    
    def _check_rate_limit(self) -> bool:
        """Check if request is within rate limit"""
        current_time = time.time()
        
        with self.lock:
            # Remove old requests (older than 1 second)
            while self.request_times and current_time - self.request_times[0] > 1.0:
                self.request_times.popleft()
            
            # Check if we're at rate limit
            if len(self.request_times) >= self.rate_limit:
                return False
            
            # Add current request
            self.request_times.append(current_time)
            return True
    
    def _get_cached_result(self, text: str) -> dict:
        """Get cached sentiment result"""
        cache_key = f"sentiment:{hash(text)}"
        
        # Try Redis first
        if self.redis_client:
            try:
                cached = self.redis_client.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception as e:
                logger.warning(f"Redis cache error: {e}")
        
        # Try in-memory cache
        return self.cache.get(text)
    
    def _cache_result(self, text: str, result: dict):
        """Cache sentiment result"""
        cache_key = f"sentiment:{hash(text)}"
        
        # Cache in Redis
        if self.redis_client:
            try:
                self.redis_client.setex(cache_key, 3600, json.dumps(result))  # 1 hour TTL
            except Exception as e:
                logger.warning(f"Redis cache error: {e}")
        
        # Cache in memory
        self.cache[text] = result
    
    def _simulate_sentiment_analysis(self, text: str) -> dict:
        """Simulate sentiment analysis with text length-based delay"""
        start_time = time.time()
        
        # Simulate processing time based on text length
        processing_time = min(len(text) * 0.01, 2.0)  # Max 2 seconds
        time.sleep(processing_time)
        
        # Simple sentiment analysis (in real implementation, use ML model)
        positive_words = ['güzel', 'harika', 'mükemmel', 'lezzetli', 'hızlı', 'temiz', 'iyi', 'beğendim']
        negative_words = ['kötü', 'berbat', 'yavaş', 'kirli', 'kötü', 'beğenmedim', 'kötü', 'rezalet']
        
        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            sentiment = 'positive'
            confidence = min(0.5 + (positive_count - negative_count) * 0.1, 0.95)
        elif negative_count > positive_count:
            sentiment = 'negative'
            confidence = min(0.5 + (negative_count - positive_count) * 0.1, 0.95)
        else:
            sentiment = 'neutral'
            confidence = 0.5
        
        actual_time = time.time() - start_time
        
        return {
            'sentiment': sentiment,
            'confidence': confidence,
            'processing_time': actual_time
        }
    
    def _random_drop(self) -> bool:
        """Randomly drop requests (5% chance)"""
        return random.random() < 0.05
    
    def AnalyzeSentiment(self, request, context):
        """Analyze sentiment of text"""
        start_time = time.time()
        request_id = request.request_id or f"req_{int(time.time() * 1000)}"
        
        logger.info(f"Received sentiment analysis request: {request_id}")
        
        # Update statistics
        with self.lock:
            self.stats['total_requests'] += 1
        
        # Check rate limit
        if not self._check_rate_limit():
            logger.warning(f"Rate limit exceeded for request: {request_id}")
            with self.lock:
                self.stats['rate_limited_requests'] += 1
            
            return sentiment_pb2.SentimentResponse(
                sentiment="neutral",
                confidence=0.0,
                processing_time=0.0,
                request_id=request_id,
                status="rate_limited",
                error_message="Rate limit exceeded"
            )
        
        # Random drop
        if self._random_drop():
            logger.warning(f"Randomly dropping request: {request_id}")
            with self.lock:
                self.stats['dropped_requests'] += 1
            
            return sentiment_pb2.SentimentResponse(
                sentiment="neutral",
                confidence=0.0,
                processing_time=0.0,
                request_id=request_id,
                status="dropped",
                error_message="Request randomly dropped"
            )
        
        # Check cache
        cached_result = self._get_cached_result(request.text)
        if cached_result:
            logger.info(f"Cache hit for request: {request_id}")
            with self.lock:
                self.stats['cached_requests'] += 1
                self.stats['successful_requests'] += 1
            
            return sentiment_pb2.SentimentResponse(
                sentiment=cached_result['sentiment'],
                confidence=cached_result['confidence'],
                processing_time=time.time() - start_time,
                request_id=request_id,
                status="success",
                error_message=""
            )
        
        # Perform sentiment analysis
        try:
            result = self._simulate_sentiment_analysis(request.text)
            
            # Cache result
            self._cache_result(request.text, result)
            
            # Update statistics
            with self.lock:
                self.stats['successful_requests'] += 1
                self.stats['average_response_time'] = (
                    (self.stats['average_response_time'] * (self.stats['successful_requests'] - 1) + 
                     (time.time() - start_time)) / self.stats['successful_requests']
                )
            
            logger.info(f"Successfully analyzed sentiment for request: {request_id}")
            
            return sentiment_pb2.SentimentResponse(
                sentiment=result['sentiment'],
                confidence=result['confidence'],
                processing_time=time.time() - start_time,
                request_id=request_id,
                status="success",
                error_message=""
            )
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment for request {request_id}: {e}")
            return sentiment_pb2.SentimentResponse(
                sentiment="neutral",
                confidence=0.0,
                processing_time=time.time() - start_time,
                request_id=request_id,
                status="error",
                error_message=str(e)
            )
    
    def GetServiceStatus(self, request, context):
        """Get service status and statistics"""
        with self.lock:
            current_rps = len(self.request_times)
            status = "healthy" if current_rps < self.rate_limit * 0.8 else "unhealthy"
            
            return sentiment_pb2.StatusResponse(
                status=status,
                requests_per_second=current_rps,
                total_requests=self.stats['total_requests'],
                average_response_time=self.stats['average_response_time'],
                rate_limit=self.rate_limit,
                current_queue_size=len(self.request_times)
            )

def serve():
    """Start gRPC server"""
    logger.info("Starting gRPC Sentiment Analysis Server...")
    
    try:
        # Create server with thread pool
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        logger.debug("Created gRPC server with thread pool")
        
        # Add servicer to server
        servicer = SentimentAnalysisServicer()
        sentiment_pb2_grpc.add_SentimentAnalysisServicer_to_server(servicer, server)
        logger.debug("Added SentimentAnalysisServicer to server")
        
        # Listen on all interfaces
        listen_addr = '[::]:50051'
        server.add_insecure_port(listen_addr)
        logger.info(f"Server listening on {listen_addr}")
        
        # Start server
        server.start()
        logger.info("gRPC Sentiment Analysis Server started successfully on port 50051")
        
        # Log server status
        logger.info("Server is ready to accept requests")
        logger.info(f"Rate limit: {servicer.rate_limit} requests/second")
        logger.info(f"Max workers: 10")
        
        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping server...")
            server.stop(0)
            logger.info("Server stopped successfully")
            
    except Exception as e:
        logger.error(f"Failed to start gRPC server: {e}")
        raise

if __name__ == '__main__':
    serve() 