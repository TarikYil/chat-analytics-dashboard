from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class ChatMessage(BaseModel):
    user_id: str
    message: str
    memory_type: str = "STM"
    timestamp: Optional[datetime] = None

class ChatResponse(BaseModel):
    response: str
    memory_type: str
    user_id: str
    timestamp: datetime

class KafkaMessage(BaseModel):
    user_id: str
    message: str
    memory_type: str
    timestamp: str

class HealthCheck(BaseModel):
    status: str
    services: List[str] 