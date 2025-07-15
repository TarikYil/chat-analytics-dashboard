#!/usr/bin/env python3
"""
Kafka Producer - LLM Yorum Üretici
Otomatik olarak yorumlar üretir ve Kafka'ya gönderir
"""

import asyncio
import logging
import sys
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from typing import Dict, Any

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KafkaConfig
from comment_generator import CommentGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Kafka Producer API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global comment generator
comment_generator = None

class CommentRequest(BaseModel):
    category: str = "menu_variety"
    sentiment: str = "positive"
    text: str = None

@app.on_event("startup")
async def startup_event():
    """Initialize comment generator on startup"""
    global comment_generator
    try:
        comment_generator = CommentGenerator()
        logger.info("Comment generator initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize comment generator: {e}")

@app.post("/send")
async def send_comment(request: CommentRequest):
    """Send a comment to Kafka"""
    try:
        if not comment_generator:
            raise HTTPException(status_code=500, detail="Comment generator not initialized")
        
        # Generate comment if text not provided
        if not request.text:
            comment = await comment_generator.generate_comment(request.category, request.sentiment)
        else:
            comment = request.text
        
        # Create message
        message = comment_generator.create_comment_message(comment, request.category, request.sentiment)
        
        # Send to Kafka
        success = comment_generator.send_to_kafka(message)
        
        if success:
            return {
                "status": "success",
                "message": "Comment sent to Kafka",
                "data": message
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send comment to Kafka")
            
    except Exception as e:
        logger.error(f"Error sending comment: {e}")
        raise HTTPException(status_code=500, detail=f"Error sending comment: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "kafka-producer",
        "kafka_connected": comment_generator is not None
    }

@app.get("/generate")
async def generate_comment(category: str = "menu_variety", sentiment: str = "positive"):
    """Generate and send a comment"""
    try:
        if not comment_generator:
            raise HTTPException(status_code=500, detail="Comment generator not initialized")
        
        # Generate comment
        comment = await comment_generator.generate_comment(category, sentiment)
        
        # Create message
        message = comment_generator.create_comment_message(comment, category, sentiment)
        
        # Send to Kafka
        success = comment_generator.send_to_kafka(message)
        
        if success:
            return {
                "status": "success",
                "message": "Comment generated and sent to Kafka",
                "data": message
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send comment to Kafka")
            
    except Exception as e:
        logger.error(f"Error generating comment: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating comment: {str(e)}")

async def run_continuous_generation():
    """Run continuous comment generation in background"""
    if comment_generator:
        await comment_generator.run_continuous_generation()

@app.on_event("startup")
async def start_background_tasks():
    """Start background tasks"""
    asyncio.create_task(run_continuous_generation())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001) 