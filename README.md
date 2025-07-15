# AI Chatbot with Kafka Integration & Analytics Dashboard

This project is a comprehensive system that includes an AI Chatbot, Kafka integration, and an advanced analytics dashboard.

## Features

### Analytics Dashboard
- **Real-time Analytics:** Live data visualization
- **Sentiment Analysis:** Sentiment analysis charts
- **Category Distribution:** Category distribution
- **Time Series Analysis:** Time series analysis
- **Interactive Charts:** Interactive charts based on Plotly

### Kafka Integration
- **Producer Service:** Comment generation and sending
- **Consumer Service:** Message listening and processing
- **REST APIs:** API endpoints for Kafka services
- **Real-time Processing:** Real-time data processing

### REST API
- **Comments API:** List processed comments
- **Filtering:** Filtering by category and sentiment
- **Statistics:** Comment statistics
- **Health Checks:** Service health monitoring

### PostgreSQL Database
- **Docker Container:** PostgreSQL 15 Alpine
- **Persistent Storage:** Volume-based data persistence
- **Health Checks:** Database health monitoring
- **Initialization Script:** Automatic schema creation

## Project Structure

```
comment_analysis_1/
├── comment_analysis_1/
│   ├── api-service/
│   │   ├── app/
│   │   │   ├── __init__.py
│   │   │   ├── database/
│   │   │   │   ├── __init__.py
│   │   │   │   └── schema.py
│   │   │   ├── main.py
│   │   │   └── models.py
│   │   ├── Dockerfile.api
│   │   ├── Dockerfile.dashboard
│   │   ├── requirements.txt
│   │   └── ui/
│   │       ├── analytics_client.py
│   │       ├── analytics_pb2_grpc.py
│   │       ├── analytics_pb2.py
│   │       ├── dashboard.py
│   │       └── kafka_consumer.py
│   ├── docker-compose.yml
│   ├── grpc-sentiment-service/
│   │   ├── analytics_client.py
│   │   ├── analytics_pb2_grpc.py
│   │   ├── analytics_pb2.py
│   │   ├── analytics_server.py
│   │   ├── Dockerfile
│   │   ├── Dockerfile.analytics
│   │   ├── proto/
│   │   │   ├── analytics.proto
│   │   │   └── sentiment.proto
│   │   ├── requirements.txt
│   │   ├── sentiment_client.py
│   │   └── sentiment_server.py
│   ├── kafka-services/
│   │   ├── analytics_consumer.py
│   │   ├── config.py
│   │   ├── consumer/
│   │   │   ├── __init__.py
│   │   │   ├── comment_processor.py
│   │   │   ├── kafka_consumer.py
│   │   │   ├── main.py
│   │   │   └── message_processor.py
│   │   ├── Dockerfile.analytics-consumer
│   │   ├── Dockerfile.consumer
│   │   ├── Dockerfile.producer
│   │   ├── producer/
│   │   │   ├── __init__.py
│   │   │   ├── comment_generator.py
│   │   │   ├── config.py
│   │   │   ├── kafka_producer.py
│   │   │   ├── main.py
│   │   │   └── message_generator.py
│   │   ├── requirements.txt
│   │   └── shared/
│   │       ├── __init__.py
│   │       ├── kafka_client.py
│   │       └── utils.py
│   ├── README.md
│   └── telegram-service/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── telegram_reporter.py
├── docker-compose.yml


```

## Installation

### Requirements
- Docker and Docker Compose
- Google Gemini API Key

### Steps

1. **Clone the project:**
   ```bash
   git clone <repository-url>
   cd comment_analysis_1
   ```

2. **Create environment variables:**
   ```bash
   # Create .env file
   echo "GOOGLE_API_KEY=your_gemini_api_key_here" > .env
   ```

3. **Start the services:**
   ```bash
   docker-compose up --build -d
   ```

4. **Wait for the services to be ready:**
   ```bash
   docker-compose logs -f
   ```

## Service Access

| Service | URL | Description |
|---------|-----|-------------|
| **API** | http://localhost:8000 | FastAPI backend |
| **Analytics Dashboard** | http://localhost:8502 | Advanced analytics dashboard |
| **Kafka Producer API** | http://localhost:8001 | Kafka producer REST API |
| **Kafka Consumer API** | http://localhost:8002 | Kafka consumer REST API |
| **PostgreSQL** | localhost:5432 | Database |
| **Redis** | localhost:6379 | Cache |
| **Kafka** | localhost:9092 | Message broker |

## PostgreSQL Database

### Docker Container Features
- **Image:** `postgres:15-alpine`
- **Database:** `chatbot_db`
- **User:** `chatbot_user`
- **Password:** `chatbot_pass`
- **Port:** `5432`

### Data Persistence
- **Volume:** `postgres_data:/var/lib/postgresql/data`
- **Init Script:** `init-db.sql` runs automatically
- **Health Check:** `pg_isready` for health monitoring

### Database Schema
```sql
-- Conversations table (for LTM)
CREATE TABLE conversations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    is_bot BOOLEAN DEFAULT FALSE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    embedding_vector REAL[]
);

-- Processed comments table (for REST API)
CREATE TABLE processed_comments (
    id SERIAL PRIMARY KEY,
    commentId VARCHAR(255) UNIQUE NOT NULL,
    message TEXT NOT NULL,
    sentiment VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    user_id VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    language VARCHAR(10) DEFAULT 'tr',
    confidence_score DECIMAL(5,4),
    processing_status VARCHAR(50) DEFAULT 'processed'
);
```

## Dashboard Features

### Real-time Analytics
- **Sentiment Distribution:** Sentiment analysis distribution
- **Category Analysis:** Category-based analysis
- **Time Series:** Time-based trend analysis
- **Quick Stats:** Quick statistics

### API Health Monitoring
- **Service Status:** Status of all services
- **Response Times:** API response times
- **Error Tracking:** Error tracking

### Interactive Features
- **Filter Controls:** Category and sentiment filters
- **Test Messages:** Send test messages
- **Auto-refresh:** Automatic data refresh

## REST API Endpoints

### API (Port 8000)
```
POST /chat                    # Chat endpoint
GET  /health                  # Health check
GET  /stats                   # System statistics
DELETE /memory/{user_id}      # Clear memory
GET  /comments                # Get comments with filtering
GET  /comments/{comment_id}   # Get specific comment
GET  /comments/stats          # Comments statistics
POST /comments                # Create new comment
```

### Kafka Producer API (Port 8001)
```
POST /send                    # Send single message
POST /send/batch              # Send batch messages
GET  /health                  # Health check
```

### Kafka Consumer API (Port 8002)
```
GET  /messages                # Get recent messages
GET  /messages/stats          # Message statistics
POST /consume                 # Manually consume messages
GET  /health                  # Health check
```

## Usage Examples

### Dashboard Usage
1. Access the **Analytics Dashboard**: http://localhost:8502
2. Set filters from the **Sidebar**
3. Review **Real-time charts**
4. Send **Test messages**

### API Usage
```bash
# Comments API
curl "http://localhost:8000/comments?category=positive&limit=10"

# Create new comment
curl -X POST "http://localhost:8000/comments" \
  -H "Content-Type: application/json" \
  -d '{
    "commentId": "comment_011",
    "message": "New comment",
    "sentiment": "positive",
    "category": "service_speed",
    "user_id": "user_011"
  }'

# Kafka Producer
curl -X POST "http://localhost:8001/send" \
  -H "Content-Type: application/json" \
  -d '{"topic": "raw-comments", "message": {"text": "Test message"}}'

# Kafka Consumer
curl "http://localhost:8002/messages?limit=5"
```

### Database Connection
```bash
# Connect to PostgreSQL
docker exec -it chatbot_postgres psql -U chatbot_user -d chatbot_db

# List tables
\dt

# Sample query
SELECT * FROM processed_comments LIMIT 5;
```

## 🔧 Development

### Adding New Features
1. Develop in the relevant service folder
2. Rebuild Docker images
3. Test with Docker Compose

### Log Monitoring
```bash
# Monitor logs of all services
docker-compose logs -f

# Monitor logs of a specific service
docker-compose logs -f api-service

# PostgreSQL logs
docker-compose logs -f postgres
```

### Database Management
```bash
# Database backup
docker exec chatbot_postgres pg_dump -U chatbot_user chatbot_db > backup.sql

# Database restore
docker exec -i chatbot_postgres psql -U chatbot_user chatbot_db < backup.sql

# Database reset
docker-compose down -v
docker-compose up -d
```

## Troubleshooting

### Service Startup Issues
1. **Port conflict:** Check the ports
2. **API Key:** Check your Google Gemini API key
3. **Database:** Check PostgreSQL connection

### Database Issues
1. **Connection refused:** Check if the PostgreSQL container is running
2. **Permission denied:** Check database user permissions
3. **Volume issues:** Check Docker volumes

### Dashboard Issues
1. **API connection:** Check API URLs
2. **Data loading:** Check database connection
3. **Charts:** Check Plotly dependencies

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Create a Pull Request 
