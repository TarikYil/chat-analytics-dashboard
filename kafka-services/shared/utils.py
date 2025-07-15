import json
import uuid
from datetime import datetime
from typing import Dict, Any

def generate_message_id() -> str:
    """Generate unique message ID"""
    return str(uuid.uuid4())

def format_timestamp() -> str:
    """Get current timestamp in ISO format"""
    return datetime.now().isoformat()

def validate_message(message: Dict[str, Any]) -> bool:
    """Validate message structure"""
    required_fields = ['timestamp', 'message_id']
    
    for field in required_fields:
        if field not in message:
            return False
    
    return True

def create_message(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized message format"""
    return {
        "message_id": generate_message_id(),
        "timestamp": format_timestamp(),
        "data": data
    }

def parse_message(message_bytes: bytes) -> Dict[str, Any]:
    """Parse message from bytes"""
    try:
        return json.loads(message_bytes.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Invalid message format: {e}") 