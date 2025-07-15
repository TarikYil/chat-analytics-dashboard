import sys
import os

# Add the parent directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from datetime import datetime
import logging
from typing import Optional, List, Dict, Any
from app.database.schema import init_database, get_comments_with_filters, get_comments_stats, check_database_health, insert_comment

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Comment Analysis API",
    description="Comment Analysis and Management API",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database connection

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Comment Analysis API is running", "status": "ok"}

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        init_database()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")



@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check database health
        db_health = check_database_health()
        
        return {
            "status": "healthy" if db_health["status"] == "healthy" else "unhealthy",
            "services": ["api", "database"],
            "database": db_health,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }





@app.get("/comments")
async def get_comments(
    category: Optional[str] = Query(None, description="Filter by category"),
    sentiment: Optional[str] = Query(None, description="Filter by sentiment"),
    limit: Optional[int] = Query(100, description="Limit number of results"),
    offset: Optional[int] = Query(0, description="Offset for pagination")
):
    """Get processed comments with filtering"""
    try:
        comments = get_comments_with_filters(
            category=category,
            sentiment=sentiment,
            limit=limit,
            offset=offset
        )
        
        return {
            "status": "success",
            "data": comments,
            "total": len(comments),
            "filters": {
                "category": category,
                "sentiment": sentiment,
                "limit": limit,
                "offset": offset
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting comments: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving comments")

@app.get("/comments/{comment_id}")
async def get_comment(comment_id: str):
    """Get specific comment by ID"""
    try:
        comments = get_comments_with_filters(limit=1)
        # Filter by commentId
        comment = next((c for c in comments if c["commentId"] == comment_id), None)
        
        if not comment:
            raise HTTPException(status_code=404, detail="Comment not found")
        
        return {
            "status": "success",
            "data": comment
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting comment {comment_id}: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving comment")

@app.get("/comments/stats")
async def get_comments_stats_endpoint():
    """Get comments statistics"""
    try:
        stats = get_comments_stats()
        return {
            "status": "success",
            "data": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting comments stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving statistics")

@app.post("/comments")
async def create_comment(comment: dict):
    """Create a new comment"""
    try:
        comment_id = comment.get("commentId")
        message = comment.get("message")
        sentiment = comment.get("sentiment")
        category = comment.get("category")
        user_id = comment.get("user_id")
        
        if not all([comment_id, message, sentiment, category]):
            raise HTTPException(status_code=400, detail="Missing required fields")
        
        success = insert_comment(comment_id, message, sentiment, category, user_id)
        
        if success:
            return {
                "status": "success",
                "message": "Comment created successfully",
                "commentId": comment_id
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create comment")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating comment: {e}")
        raise HTTPException(status_code=500, detail="Error creating comment")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 