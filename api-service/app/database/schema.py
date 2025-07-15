import psycopg2
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Database configuration for Docker
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'chatbot_db'),
    'user': os.getenv('POSTGRES_USER', 'chatbot_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'chatbot_pass')
}

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def init_database():
    """Initialize database tables"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create conversations table (for LTM)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                message TEXT NOT NULL,
                is_bot BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                embedding_vector REAL[]
            )
        """)
        
        # Create processed_comments table (for REST API)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_comments (
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
            )
        """)
        
        # Create indexes for better performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations(user_id);
            CREATE INDEX IF NOT EXISTS idx_conversations_timestamp ON conversations(timestamp);
            CREATE INDEX IF NOT EXISTS idx_processed_comments_sentiment ON processed_comments(sentiment);
            CREATE INDEX IF NOT EXISTS idx_processed_comments_category ON processed_comments(category);
            CREATE INDEX IF NOT EXISTS idx_processed_comments_timestamp ON processed_comments(timestamp);
            CREATE INDEX IF NOT EXISTS idx_processed_comments_user_id ON processed_comments(user_id);
        """)
        
        # Insert sample data if table is empty
        cursor.execute("SELECT COUNT(*) FROM processed_comments")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insert sample comments
            sample_comments = [
                ("comment_001", "Harika bir deneyimdi, kesinlikle tekrar geleceğim!", "positive", "service_speed", "user_001"),
                ("comment_002", "Yemekler çok lezzetliydi, servis de hızlıydı.", "positive", "taste_quality", "user_002"),
                ("comment_003", "Fiyatlar biraz yüksek ama kalite iyi.", "neutral", "price_performance", "user_003"),
                ("comment_004", "Ortam çok güzeldi, romantik bir akşam yemeği için ideal.", "positive", "ambiance", "user_004"),
                ("comment_005", "Menüde çok fazla seçenek var, herkes için bir şey bulunur.", "positive", "menu_variety", "user_005"),
                ("comment_006", "Servis biraz yavaştı ama yemekler değerdi.", "neutral", "service_speed", "user_006"),
                ("comment_007", "Temizlik konusunda daha dikkatli olabilirler.", "negative", "ambiance", "user_007"),
                ("comment_008", "Personel çok nazik ve yardımseverdi.", "positive", "service_speed", "user_008"),
                ("comment_009", "Porsiyonlar küçük ama lezzetliydi.", "neutral", "taste_quality", "user_009"),
                ("comment_010", "Rezervasyon yapmak gerekiyor, çok kalabalık.", "neutral", "service_speed", "user_010")
            ]
            
            cursor.executemany("""
                INSERT INTO processed_comments (commentId, message, sentiment, category, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, sample_comments)
            
            logger.info("Sample comments inserted")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

def check_database_health() -> dict:
    """Check database health and return status"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('conversations', 'processed_comments')
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get row counts
        cursor.execute("SELECT COUNT(*) FROM conversations")
        conversations_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processed_comments")
        comments_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "tables": tables,
            "conversations_count": conversations_count,
            "comments_count": comments_count,
            "connection": "success"
        }
        
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "connection": "failed"
        }

def insert_comment(comment_id: str, message: str, sentiment: str, category: str, user_id: Optional[str] = None):
    """Insert a new comment into processed_comments table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO processed_comments (commentId, message, sentiment, category, user_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (commentId) DO UPDATE SET
                message = EXCLUDED.message,
                sentiment = EXCLUDED.sentiment,
                category = EXCLUDED.category,
                timestamp = CURRENT_TIMESTAMP
        """, (comment_id, message, sentiment, category, user_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Comment {comment_id} inserted/updated successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting comment {comment_id}: {e}")
        return False

def get_comments_with_filters(category: Optional[str] = None, 
                            sentiment: Optional[str] = None, 
                            limit: int = 100, 
                            offset: int = 0) -> list:
    """Get comments with optional filters"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query
        query = """
            SELECT 
                commentId,
                message as text,
                sentiment,
                category,
                timestamp,
                user_id
            FROM processed_comments
            WHERE 1=1
        """
        params = []
        
        if category:
            query += " AND category = %s"
            params.append(category)
        
        if sentiment:
            query += " AND sentiment = %s"
            params.append(sentiment)
        
        query += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        comments = []
        for row in results:
            comments.append({
                "commentId": row[0],
                "text": row[1],
                "sentiment": row[2],
                "category": row[3],
                "timestamp": row[4].isoformat() if row[4] else None,
                "user_id": row[5]
            })
        
        cursor.close()
        conn.close()
        
        return comments
        
    except Exception as e:
        logger.error(f"Error getting comments: {e}")
        return []

def get_comments_stats() -> dict:
    """Get comments statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM processed_comments")
        total_count = cursor.fetchone()[0]
        
        # Get sentiment distribution
        cursor.execute("""
            SELECT sentiment, COUNT(*) 
            FROM processed_comments 
            GROUP BY sentiment
        """)
        sentiment_stats = dict(cursor.fetchall())
        
        # Get category distribution
        cursor.execute("""
            SELECT category, COUNT(*) 
            FROM processed_comments 
            GROUP BY category
        """)
        category_stats = dict(cursor.fetchall())
        
        # Get recent activity
        cursor.execute("""
            SELECT COUNT(*) 
            FROM processed_comments 
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """)
        recent_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "total_comments": total_count,
            "recent_comments_24h": recent_count,
            "sentiment_distribution": sentiment_stats,
            "category_distribution": category_stats
        }
        
    except Exception as e:
        logger.error(f"Error getting comments stats: {e}")
        return {}

if __name__ == "__main__":
    init_database() 