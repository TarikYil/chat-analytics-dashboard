import streamlit as st

# Page configuration - MUST be first Streamlit command
st.set_page_config(
    page_title="AI Chatbot Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import asyncio
from typing import Dict, Any, List
import redis
import grpc
import sys
import os
import threading

# Import Kafka consumer
try:
    from kafka_consumer import get_kafka_consumer, start_kafka_consumer, stop_kafka_consumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Kafka consumer başlatma işlemi (artık mesaj yok)
if 'KAFKA_AVAILABLE' in globals() and KAFKA_AVAILABLE:
    try:
        consumer = get_kafka_consumer()
        if not getattr(consumer, 'is_running', False) and not st.session_state.get('kafka_consumer_started', False):
            consumer.start_consuming()
            st.session_state.kafka_consumer_started = True
        elif getattr(consumer, 'is_running', False):
            st.session_state.kafka_consumer_started = True
        else:
            pass
    except Exception:
        st.session_state.kafka_consumer_started = False

# Import Analytics client
try:
    import sys
    import os
    try:
        import analytics_pb2
        import analytics_pb2_grpc
    except ImportError:
        ANALYTICS_AVAILABLE = False
        raise
    try:
        from analytics_client import AnalyticsClient
        ANALYTICS_AVAILABLE = True
    except ImportError:
        ANALYTICS_AVAILABLE = False
        raise
except ImportError:
    ANALYTICS_AVAILABLE = False

# gRPC protobuf import
sys.path.insert(0, os.getcwd())
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    import sentiment_pb2
    import sentiment_pb2_grpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    sentiment_pb2 = None
    sentiment_pb2_grpc = None

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "kafka_messages" not in st.session_state:
    st.session_state.kafka_messages = []
if "last_update" not in st.session_state:
    st.session_state.last_update = datetime.now()
if "service_alerts" not in st.session_state:
    st.session_state.service_alerts = []
if "kafka_consumer_started" not in st.session_state:
    st.session_state.kafka_consumer_started = False

class DashboardManager:
    def __init__(self):
        self.api_base_url = "http://api:8000"  # AI API disabled
        self.kafka_producer_url = "http://kafka-producer:8001"
        self.kafka_consumer_url = "http://kafka-consumer:8002"
        self.grpc_host = "grpc-sentiment-service"
        self.grpc_port = 50051
        self.analytics_host = "grpc-analytics-service"
        self.analytics_port = 50052
        self.redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
        self.grpc_stub = None
        if GRPC_AVAILABLE:
            try:
                self.grpc_channel = grpc.insecure_channel(f'{self.grpc_host}:{self.grpc_port}')
                self.grpc_stub = sentiment_pb2_grpc.SentimentAnalysisStub(self.grpc_channel)
            except Exception as e:
                self.grpc_stub = None
        self.analytics_client = None
        if ANALYTICS_AVAILABLE:
            try:
                self.analytics_client = AnalyticsClient(host=self.analytics_host, port=self.analytics_port)
            except Exception as e:
                self.analytics_client = None

    def check_kafka_producer_health(self) -> dict:
        try:
            resp = requests.get(f"{self.kafka_producer_url}/health", timeout=3)
            if resp.status_code == 200:
                return {"status": "healthy"}
            else:
                return {"status": "unhealthy", "status_code": resp.status_code}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_kafka_consumer_health(self) -> dict:
        try:
            resp = requests.get(f"{self.kafka_consumer_url}/health", timeout=3)
            if resp.status_code == 200:
                return {"status": "healthy"}
            else:
                return {"status": "unhealthy", "status_code": resp.status_code}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_redis_health(self) -> dict:
        try:
            pong = self.redis_client.ping()
            if pong:
                return {"status": "healthy"}
            else:
                return {"status": "unhealthy"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_api_health(self) -> Dict[str, Any]:
        try:
            response = requests.get(f"{self.api_base_url}/health", timeout=5)
            return {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "response_time": response.elapsed.total_seconds(),
                "status_code": response.status_code
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "response_time": None,
                "status_code": None
            }

    def check_grpc_health(self) -> Dict[str, Any]:
        if not GRPC_AVAILABLE:
            return {"status": "error", "error": "gRPC protobuf files not available"}
        try:
            if not self.grpc_stub:
                return {"status": "error", "error": "gRPC client not initialized"}
            request = sentiment_pb2.StatusRequest(service_name="sentiment_analysis")
            response = self.grpc_stub.GetServiceStatus(request)
            return {
                "status": response.status,
                "requests_per_second": response.requests_per_second,
                "total_requests": response.total_requests,
                "average_response_time": response.average_response_time,
                "rate_limit": response.rate_limit,
                "current_queue_size": response.current_queue_size
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def analyze_sentiment_grpc(self, text: str) -> Dict[str, Any]:
        if not GRPC_AVAILABLE:
            return {"error": "gRPC protobuf files not available"}
        try:
            if not self.grpc_stub:
                return {"error": "gRPC client not available"}
            request = sentiment_pb2.SentimentRequest(
                text=text,
                language="tr",
                request_id=f"dashboard_{int(time.time() * 1000)}"
            )
            response = self.grpc_stub.AnalyzeSentiment(request)
            return {
                "sentiment": response.sentiment,
                "confidence": response.confidence,
                "processing_time": response.processing_time,
                "status": response.status,
                "error_message": response.error_message
            }
        except Exception as e:
            return {"error": str(e)}

    def get_system_stats(self) -> Dict[str, Any]:
        try:
            response = requests.get(f"{self.api_base_url}/stats", timeout=5)
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            return {"error": str(e)}

    def get_comments(self, category: str = None, sentiment: str = None) -> List[Dict[str, Any]]:
        try:
            params = {}
            if category:
                params['category'] = category
            if sentiment:
                params['sentiment'] = sentiment
            response = requests.get(f"{self.api_base_url}/comments", params=params, timeout=10)
            if response.status_code == 200:
                return response.json().get("data", [])
            return []
        except Exception as e:
            return []

    def get_realtime_stats(self) -> Dict[str, Any]:
        try:
            stats = {
                "total_processed": int(self.redis_client.get("stats:total") or 0),
                "positive": int(self.redis_client.get("stats:positive") or 0),
                "negative": int(self.redis_client.get("stats:negative") or 0),
                "neutral": int(self.redis_client.get("stats:neutral") or 0),
                "last_update": datetime.now().isoformat()
            }
            return stats
        except Exception as e:
            return {"error": str(e)}

    def get_all_analytics(self, time_range: str = "24h", interval: str = "1h", 
                         category_filter: str = "", sentiment_filter: str = "", 
                         keyword_limit: int = 10) -> Dict[str, Any]:
        if not ANALYTICS_AVAILABLE:
            return {"error": "Analytics client not available"}
        try:
            if not self.analytics_client:
                return {"error": "Analytics client not initialized"}
            analytics_data = self.analytics_client.get_all_analytics(
                time_range=time_range,
                interval=interval,
                category_filter=category_filter,
                sentiment_filter=sentiment_filter,
                keyword_limit=keyword_limit
            )
            return analytics_data
        except Exception as e:
            return {"error": str(e)}

    def get_recent_comments_from_cache(self, sentiment: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        try:
            if sentiment:
                cache_key = f"recent_comments:{sentiment}"
                comments = self.redis_client.lrange(cache_key, 0, limit-1)
            else:
                comments = []
                for sent in ["positive", "negative", "neutral"]:
                    cache_key = f"recent_comments:{sent}"
                    sent_comments = self.redis_client.lrange(cache_key, 0, limit//3-1)
                    comments.extend(sent_comments)
            parsed_comments = []
            for comment_json in comments:
                try:
                    parsed_comments.append(json.loads(comment_json))
                except:
                    continue
            return parsed_comments
        except Exception as e:
            return []

    def send_test_message(self, message: str, user_id: str = "dashboard_user"):
        try:
            kafka_message = {
                "user_id": user_id,
                "message": message,
                "memory_type": "STM",
                "timestamp": datetime.now().isoformat(),
                "source": "dashboard"
            }
            response = requests.post(
                f"{self.kafka_producer_url}/send",
                json={"topic": "chat-messages", "message": kafka_message},
                timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            return False



def create_service_status_card(service_name: str, status: str, details: Dict[str, Any]):
    """Create a service status card"""
    if status == "healthy":
        st.success(f"✅ {service_name}")
    elif status == "unhealthy":
        st.error(f"❌ {service_name}")
    else:
        st.warning(f"⚠️ {service_name}")
    
    if details:
        for key, value in details.items():
            if key != "status":
                st.metric(key.replace("_", " ").title(), value)

def show_navbar_stats():
    try:
        redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
        positive_count = int(redis_client.get('positive_comments') or 0)
        negative_count = int(redis_client.get('negative_comments') or 0)
        neutral_count = int(redis_client.get('neutral_comments') or 0)
        total_comments = int(redis_client.get('total_comments') or 0)
        st.markdown(f"""
        <div style='background-color:#f0f2f6;padding:10px 0 10px 0;margin-bottom:10px;border-radius:8px;display:flex;justify-content:space-around;font-size:1.1em;'>
            <b>Toplam:</b> {total_comments} &nbsp;|&nbsp; 
            <span style='color:#2ecc40'><b>Positive:</b> {positive_count}</span> &nbsp;|&nbsp; 
            <span style='color:#ff4136'><b>Negative:</b> {negative_count}</span> &nbsp;|&nbsp; 
            <span style='color:#ffdc00'><b>Neutral:</b> {neutral_count}</span>
        </div>
        """, unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"Navbar istatistikleri yüklenemedi: {e}")

def main():
    show_navbar_stats()
    st.header("📈 Analytics Dashboard")
    
    # Initialize dashboard manager
    dashboard = DashboardManager()
    
    # Start Kafka consumer if available
    if KAFKA_AVAILABLE:
        try:
            consumer = get_kafka_consumer()
            if not consumer.is_running:
                consumer.start_consuming()
                st.session_state.kafka_consumer_started = True
        except Exception as e:
            st.error(f"Failed to start Kafka consumer: {e}")
    else:
        st.warning("Kafka not available")
    
    # Sidebar
    with st.sidebar:
        st.header("🔧 Dashboard Controls")
        
        # Service Alerts
        st.subheader("🚨 Service Alerts")
        if st.session_state.service_alerts:
            for alert in st.session_state.service_alerts[-5:]:  # Show last 5 alerts
                if alert["type"] == "error":
                    st.error(alert["message"])
                elif alert["type"] == "warning":
                    st.warning(alert["message"])
                else:
                    st.info(alert["message"])
        else:
            st.info("No alerts")
        
        # Filters
        st.subheader("🔍 Comment Filters")
        category_filter = st.selectbox(
            "Category",
            ["All", "menu_variety", "taste_quality", "service_speed", "ambiance", "price_performance"]
        )
        sentiment_filter = st.selectbox(
            "Sentiment",
            ["All", "positive", "negative", "neutral"]
        )

    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        
        
        # Service Analysis
        st.subheader("🔍 Service Analysis")
        
        # Create service status overview
        services_status = {
            "API Service": dashboard.check_api_health(),
            "gRPC Sentiment": dashboard.check_grpc_health(),
            "Kafka Producer": dashboard.check_kafka_producer_health(),
            "Kafka Consumer": dashboard.check_kafka_consumer_health(),
            "Redis Cache": dashboard.check_redis_health(),
        }
        
        # Display service status in a grid
        status_cols = st.columns(len(services_status))
        for i, (service_name, status_info) in enumerate(services_status.items()):
            with status_cols[i]:
                status = status_info.get("status", "unknown")
                if status == "healthy":
                    st.success(f"✅ {service_name}")
                elif status == "unhealthy":
                    st.error(f"❌ {service_name}")
                else:
                    st.warning(f"⚠️ {service_name}")
        
        # Comments Analysis
        st.subheader("💬 Comments Analysis")
        
        # Get filtered comments
        category = category_filter if category_filter != "All" else None
        sentiment = sentiment_filter if sentiment_filter != "All" else None
        comments = dashboard.get_comments(category, sentiment)
        
        if comments:
            # Create DataFrame
            df = pd.DataFrame(comments)
            
            # Sentiment distribution
            if 'sentiment' in df.columns:
                sentiment_counts = df['sentiment'].value_counts()
                fig = px.pie(
                    values=sentiment_counts.values,
                    names=sentiment_counts.index,
                    title="Sentiment Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Category distribution
            if 'category' in df.columns:
                category_counts = df['category'].value_counts()
                fig = px.bar(
                    x=category_counts.index,
                    y=category_counts.values,
                    title="Comments by Category"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Recent comments table
            st.subheader("📋 Recent Comments")
            display_df = df[['commentId', 'text', 'category', 'sentiment', 'timestamp']].head(10)
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No comments found with current filters")

    with col2:
        # Sadece analytics tabları kalsın
        # Analytics Data
        st.subheader("📈 Analytics Data")
        
        # Analytics tabs
        analytics_tab1, analytics_tab2, analytics_tab3 = st.tabs([
            "😊 Sentiment", "📂 Category", "⚡ Real-time"
        ])
        
        with analytics_tab1:
            st.subheader("😊 Sentiment Analytics")
            
            # Son Yorumlar alanı (her sentiment için sadece en güncel yorum)
            try:
                redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
                st.markdown("**Son Yorumlar**")
                cols = st.columns(3)
                for idx, (sentiment, label) in enumerate(zip(["positive", "negative", "neutral"], ["Positive", "Negative", "Neutral"])):
                    key = f"last_{sentiment}_comments"
                    last_comment = redis_client.lindex(key, 0)
                    with cols[idx]:
                        st.markdown(f"**{label}:**")
                        if last_comment:
                            st.write(last_comment)
                        else:
                            st.info(f"Yorum yok")
            except Exception as e:
                st.error(f"Redis'ten son yorumlar okunamadı: {e}")
            
            # Redis'ten sentiment verilerini al
            try:
                redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
                
                # Sentiment sayılarını al
                positive_count = int(redis_client.get('positive_comments') or 0)
                negative_count = int(redis_client.get('negative_comments') or 0)
                neutral_count = int(redis_client.get('neutral_comments') or 0)
                total_comments = int(redis_client.get('total_comments') or 0)
                
                # Oranları hesapla
                positive_ratio = float(redis_client.get('positive_ratio') or 0)
                negative_ratio = float(redis_client.get('negative_ratio') or 0)
                neutral_ratio = float(redis_client.get('neutral_ratio') or 0)
                
                # Comments per minute
                comments_per_minute = float(redis_client.get('comments_per_minute') or 0)
                
                st.success("✅ Real-time sentiment data from Redis")
                
                # Metrikleri göster
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Positive", f"{positive_ratio:.1f}%", f"{positive_count} comments")
                with col2:
                    st.metric("Negative", f"{negative_ratio:.1f}%", f"{negative_count} comments")
                with col3:
                    st.metric("Neutral", f"{neutral_ratio:.1f}%", f"{neutral_count} comments")
                with col4:
                    st.metric("Total", f"{total_comments}", f"{comments_per_minute:.1f}/min")
                
                # Sentiment dağılım grafiği
                if total_comments > 0:
                    sentiment_data = {
                        'Positive': positive_count,
                        'Negative': negative_count,
                        'Neutral': neutral_count
                    }
                    
                    fig = px.pie(
                        values=list(sentiment_data.values()),
                        names=list(sentiment_data.keys()),
                        title="Real-time Sentiment Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Category breakdown from Redis
                category_keys = redis_client.keys('category_*')
                if category_keys:
                    category_data = {}
                    for key in category_keys:
                        category_name = key.replace('category_', '')
                        count = int(redis_client.get(key) or 0)
                        if count > 0:
                            category_data[category_name] = count
                    
                    if category_data:
                        st.subheader("📊 Category Breakdown")
                        fig = px.bar(
                            x=list(category_data.keys()),
                            y=list(category_data.values()),
                            title="Comments by Category"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error reading from Redis: {e}")
            
            # Kafka'dan gelen veriler (mevcut kod)
            st.subheader("📈 Kafka Analytics")
            if KAFKA_AVAILABLE:
                try:
                    consumer = get_kafka_consumer()
                    sentiment_messages = consumer.get_sentiment_messages(10)
                    
                    if sentiment_messages:
                        st.success("✅ Kafka sentiment data received!")
                        latest_sentiment = sentiment_messages[-1]
                        sentiment_data = latest_sentiment.get('data', {})
                        
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Positive", f"{sentiment_data.get('positive_percentage', 0):.1f}%")
                        with col2:
                            st.metric("Negative", f"{sentiment_data.get('negative_percentage', 0):.1f}%")
                        with col3:
                            st.metric("Neutral", f"{sentiment_data.get('neutral_percentage', 0):.1f}%")
                        
                        # Sentiment counts chart
                        if 'sentiment_counts' in sentiment_data:
                            sentiment_counts = sentiment_data['sentiment_counts']
                            fig = px.pie(
                                values=list(sentiment_counts.values()),
                                names=list(sentiment_counts.keys()),
                                title="Kafka Sentiment Distribution"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        st.text(f"Last update: {latest_sentiment.get('timestamp', 'Unknown')}")
                        
                        # Show raw data for debugging
                        with st.expander("🔍 Raw Kafka Data"):
                            st.json(latest_sentiment)
                    else:
                        st.warning("⚠️ No Kafka sentiment analytics data available")
                        
                        # Try to restart consumer
                        if st.button("🔄 Restart Kafka Consumer"):
                            try:
                                consumer.stop_consuming()
                                time.sleep(1)
                                consumer.start_consuming()
                                st.success("Consumer restarted!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to restart consumer: {e}")
                except Exception as e:
                    st.error(f"Error accessing Kafka consumer: {e}")
            else:
                st.warning("❌ Kafka consumer not available")
        
        with analytics_tab2:
            st.subheader("📂 Category Analytics")
            
            if KAFKA_AVAILABLE:
                try:
                    consumer = get_kafka_consumer()
                    category_messages = consumer.get_category_messages(10)
                    
                    if category_messages:
                        st.success("✅ Category data received!")
                        latest_category = category_messages[-1]
                        category_data = latest_category.get('data', {})
                        
                        # Display category percentages yan yana
                        if 'category_percentages' in category_data:
                            categories = category_data['category_percentages']
                            cols = st.columns(len(categories))
                            for idx, cp in enumerate(categories):
                                with cols[idx]:
                                    st.markdown(f"**{cp['category']}**")
                                    st.markdown(f"<span style='font-size:2em'>{cp['percentage']:.1f}%</span>", unsafe_allow_html=True)
                        
                        st.text(f"Last update: {latest_category.get('timestamp', 'Unknown')}")
                        
                        # Show raw data for debugging
                        with st.expander("🔍 Raw Category Data"):
                            st.json(latest_category)
                    else:
                        st.warning("⚠️ No category analytics data available")
                        
                        # Try to restart consumer
                        if st.button("🔄 Restart Kafka Consumer", key="restart_category"):
                            try:
                                consumer.stop_consuming()
                                time.sleep(1)
                                consumer.start_consuming()
                                st.success("Consumer restarted!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to restart consumer: {e}")
                except Exception as e:
                    st.error(f"Error accessing Kafka consumer: {e}")
            else:
                st.warning("❌ Kafka consumer not available")
        
        with analytics_tab3:
            st.subheader("⚡ Real-time Analytics")
            
            if KAFKA_AVAILABLE:
                try:
                    consumer = get_kafka_consumer()
                    realtime_messages = consumer.get_realtime_messages(5)
                    
                    if realtime_messages:
                        st.success("✅ Real-time data received!")
                        latest_realtime = realtime_messages[-1]
                        realtime_data = latest_realtime.get('data', {})
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Comments/min", f"{realtime_data.get('comments_per_minute', 0):.1f}")
                            st.metric("Active Consumers", realtime_data.get('active_consumers', 0))
                        with col2:
                            st.metric("Queue Size", realtime_data.get('queue_size', 0))
                            st.metric("Positive Ratio", f"{realtime_data.get('positive_ratio', 0):.1f}%")
                        with col3:
                            st.metric("Negative Ratio", f"{realtime_data.get('negative_ratio', 0):.1f}%")
                            st.metric("Neutral Ratio", f"{realtime_data.get('neutral_ratio', 0):.1f}%")
                        
                        st.text(f"Last update: {latest_realtime.get('timestamp', 'Unknown')}")
                        
                        # Show raw data for debugging
                        with st.expander("🔍 Raw Real-time Data"):
                            st.json(latest_realtime)
                    else:
                        st.warning("⚠️ No real-time analytics data available")
                        
                        # Try to restart consumer
                        if st.button("🔄 Restart Kafka Consumer", key="restart_realtime"):
                            try:
                                consumer.stop_consuming()
                                time.sleep(1)
                                consumer.start_consuming()
                                st.success("Consumer restarted!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to restart consumer: {e}")
                except Exception as e:
                    st.error(f"Error accessing Kafka consumer: {e}")
            else:
                st.warning("❌ Kafka consumer not available")
        
        # Manual analytics refresh
        if st.button("🔄 Refresh Analytics"):
            with st.spinner("Loading analytics data..."):
                analytics_data = dashboard.get_all_analytics(
                    time_range="24h", # Default to 24h for manual refresh
                    interval="1h" # Default to 1h for manual refresh
                )
                
                if "error" not in analytics_data:
                    st.success("Analytics data refreshed successfully!")
                else:
                    st.error(f"Analytics error: {analytics_data['error']}")
        
        # Last Update
        st.subheader("🕒 Last Update")
        st.text(f"Last update: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Auto-refresh
        if st.checkbox("🔄 Auto-refresh", value=False):
            st.info("Auto-refresh enabled - data will update every 30 seconds")
            time.sleep(30)
            st.rerun()

if __name__ == "__main__":
    main() 